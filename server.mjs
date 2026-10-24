import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { ProxyAgent } from "undici";

const COUNT = parseInt(process.env.RELAY_COUNT || "10", 10);
const BASE_PORT = parseInt(process.env.BASE_PORT || "3000", 10);
const TUNNEL_LOG_DIR = process.env.TUNNEL_LOG_DIR || "/var/log/cloudflared";
const HEADER_TARGET = "x-relay-target";
const HEADER_PATH = "x-relay-path";
const HEADER_HOST = "host";
const DEFAULT_ALLOWED_TARGETS = "https://opencode.ai,https://api.kilo.ai";
const ALLOWED_TARGETS = (process.env.RELAY_ALLOWED_TARGETS ?? DEFAULT_ALLOWED_TARGETS)
  .split(",")
  .map((s) => s.trim().replace(/\/$/, ""))
  .filter(Boolean);
const RESET_TOKEN = process.env.RESET_TOKEN || "bdc5eea4c71f5c995efa8d38e03c72067bc8702dbfe6f4cc7167ea459af6b7cd";
let isResetInProgress = false;

// Docker Engine API helper over unix domain socket
function dockerRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? (typeof body === "string" ? body : JSON.stringify(body)) : null;
    const req = http.request({
      socketPath: "/var/run/docker.sock",
      path,
      method,
      headers: {
        "Content-Type": "application/json",
        ...(bodyStr ? { "Content-Length": Buffer.byteLength(bodyStr) } : {})
      }
    }, (res) => {
      let data = "";
      res.on("data", chunk => data += chunk);
      res.on("end", () => {
        try {
          const json = data ? JSON.parse(data) : {};
          resolve({ status: res.statusCode, data: json, raw: data });
        } catch {
          resolve({ status: res.statusCode, data, raw: data });
        }
      });
    });
    req.on("error", reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

async function findWarpContainers() {
  try {
    const res = await dockerRequest("GET", "/containers/json?filters=" + encodeURIComponent(JSON.stringify({ name: ["warp-"] })));
    if (res.status === 200 && Array.isArray(res.data)) {
      return res.data;
    }
  } catch (err) {
    console.error(`[reset] Failed to list containers via docker socket:`, err.message);
  }
  return [];
}

function getWarpContainerForIndex(containers, index) {
  const re = new RegExp(`(^|/)warp-${index}(-[a-z0-9]+|$)`, "i");
  const found = containers.find(c => c.Names && c.Names.some(n => re.test(n)));
  return found || null;
}

async function execInContainer(containerId, cmd) {
  try {
    const createRes = await dockerRequest("POST", `/containers/${containerId}/exec`, {
      Cmd: cmd,
      AttachStdout: true,
      AttachStderr: true
    });
    if (createRes.status === 201 && createRes.data?.Id) {
      const execId = createRes.data.Id;
      await dockerRequest("POST", `/exec/${execId}/start`, {
        Detach: false,
        Tty: false
      });
      return true;
    }
  } catch (err) {
    console.error(`[reset] Exec failed on ${containerId}:`, err.message);
  }
  return false;
}

async function restartContainer(containerId) {
  try {
    const res = await dockerRequest("POST", `/containers/${containerId}/restart?t=10`);
    return res.status === 204 || res.status === 200;
  } catch (err) {
    console.error(`[reset] Restart failed on ${containerId}:`, err.message);
    return false;
  }
}

// Log-and-continue on programming errors: one bad request must never take
// down all 10 listeners in this process. Logging keeps the failure visible
// (journald/docker logs) instead of crashing or swallowing it silently.
process.on("unhandledRejection", (err) => {
  console.error(`[relay] unhandledRejection: ${err?.stack ?? String(err)}`);
});
process.on("uncaughtException", (err) => {
  console.error(`[relay] uncaughtException: ${err?.stack ?? String(err)}`);
});

const dispatchers = new Map();
const egressIps = new Map();

async function probeEgress(index) {
  const dispatcher = dispatchers.get(index);
  if (!dispatcher) return;
  try {
    const res = await fetch("https://cloudflare.com/cdn-cgi/trace", {
      dispatcher,
      signal: AbortSignal.timeout(5000),
    });
    const text = await res.text();
    const match = text.match(/^ip=(.+)$/m);
    if (match) {
      egressIps.set(index, match[1].trim());
    }
  } catch (e) {
    // leave existing or null
  }
}

async function waitForNewEgress(index, maxSeconds = 15) {
  egressIps.delete(index);
  const deadline = Date.now() + maxSeconds * 1000;
  while (Date.now() < deadline) {
    await probeEgress(index);
    if (egressIps.has(index)) return egressIps.get(index);
    await new Promise((r) => setTimeout(r, 2000));
  }
  return egressIps.get(index) || null;
}

function createRelayInstance(index) {
  const port = BASE_PORT + index;
  const warpHost = process.env[`WARP_HOST_${index}`] || `warp-${index}`;
  const warpPort = process.env[`WARP_PORT_${index}`] || "1080";
  const proxyUrl = `socks5://${warpHost}:${warpPort}`;
  const dispatcher = new ProxyAgent({ uri: proxyUrl, keepAliveTimeout: 60_000, keepAliveMaxTimeout: 90_000, headersTimeout: 60_000, bodyTimeout: 0, connections: 32 });
  dispatchers.set(index, dispatcher);

  const server = http.createServer(async (req, res) => {
    const target = req.headers[HEADER_TARGET]?.replace(/\/$/, "");
    const relayPath = req.headers[HEADER_PATH] || "/";

    // Direct health check: only when NO x-relay-target header is passed
    if (!target) {
      try {
        const trace = await fetch("https://cloudflare.com/cdn-cgi/trace", { dispatcher });
        const text = await trace.text();
        res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
        return res.end(`Relay #${index} OK (via ${proxyUrl})\n\n${text}`);
      } catch (err) {
        // Detail stays in the logs: the body used to reflect String(err).
        console.error(`[relay #${index}] health check failed: ${err?.stack ?? String(err)}`);
        res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
        return res.end(`Relay #${index} health check failed\n`);
      }
    }

    if (!ALLOWED_TARGETS.includes(target)) {
      res.writeHead(403, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "Forbidden target" }));
    }

    // The allowlist check above only validates the raw header string; the
    // request URL is target + relayPath, and a crafted path can continue the
    // host label (".attacker.tld/x" -> opencode.ai.attacker.tld), swap the
    // port (":8443/x"), or inject userinfo ("@evil.tld/x"). Build the URL
    // once and require it to stay on the allowlisted origin with a rooted
    // path. Concat (not URL-relative resolution) preserves the legacy
    // target+path semantics for any allowlist entry carrying a base path.
    let upstreamUrl;
    let targetOrigin;
    try {
      targetOrigin = new URL(target).origin;
      upstreamUrl = new URL(target + relayPath);
    } catch {
      upstreamUrl = null;
    }
    if (!upstreamUrl || upstreamUrl.origin !== targetOrigin || !relayPath.startsWith("/")) {
      console.error(`[relay #${index}] rejected relay path: target=${target} path=${relayPath}`);
      res.writeHead(400, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "Invalid relay path" }));
    }

    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (k !== HEADER_TARGET && k !== HEADER_PATH && k !== HEADER_HOST && v) {
        headers.set(k, Array.isArray(v) ? v.join(", ") : v);
      }
    }

    // 5-min upstream cap + client-disconnect wiring: a client that goes away
    // must not leave its upstream fetch hanging (leaked sockets pile up under
    // agent retries). Note req 'close' also fires on normal POST body
    // completion (socket still alive), so only abort when the socket actually
    // died; res 'close' covers disconnect-while-waiting for all methods.
    const controller = new AbortController();
    const onClientClose = () => {
      if (!res.writableEnded && req.socket?.destroyed) controller.abort();
    };
    req.once("close", onClientClose);
    res.once("close", onClientClose);
    const upstreamTimeout = AbortSignal.timeout(300000);
    try {
      const upstream = await fetch(upstreamUrl, {
        method: req.method,
        headers,
        body: req.method !== "GET" && req.method !== "HEAD" ? req : undefined,
        duplex: "half",
        dispatcher,
        signal: AbortSignal.any([upstreamTimeout, controller.signal]),
      });

      // Forward only what the client needs: full header copies cost per-request
      // work and risk stale content-encoding/content-length (fetch decodes
      // gzip/deflate/br, so those upstream values lie about our body).
      const outHeaders = { "cache-control": "no-cache", "x-accel-buffering": "no" };
      const contentType = upstream.headers.get("content-type");
      if (contentType) outHeaders["content-type"] = contentType;
      res.writeHead(upstream.status, outHeaders);
      // Kickstart: Node buffers headers until the first body byte, so TTFB
      // would wait for the first token. Flush now — headers land at
      // upstream-headers time instead.
      res.flushHeaders();
      if (upstream.body) {
        // Zero-copy pipe with backpressure; for-await + res.write() costs a
        // microtask per chunk and buffers against a slow client.
        await pipeline(Readable.fromWeb(upstream.body), res);
      } else {
        res.end();
      }
    } catch (err) {
      if (!res.headersSent) {
        // Body stays generic: String(err) reflected attacker-controlled URLs
        // and internal proxy names back to the caller.
        console.error(`[relay #${index}] upstream error: ${err?.stack ?? String(err)}`);
        res.writeHead(502, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "upstream request failed" }));
      } else {
        // Headers already on the wire (mid-stream failure) — just cut it.
        // Unconditional writeHead here crashed the process (ERR_HTTP_HEADERS_SENT).
        res.destroy();
      }
    } finally {
      req.removeListener("close", onClientClose);
      res.removeListener("close", onClientClose);
    }
  });

  // TTFT tuning: Nagle off (first small frame goes immediately), long
  // keep-alive so cloudflared reuses the relay-app connection.
  server.keepAliveTimeout = 90_000;
  server.headersTimeout = 95_000;
  server.requestTimeout = 0;
  server.timeout = 0;
  server.on("connection", (socket) => socket.setNoDelay(true));

  server.listen(port, "0.0.0.0", () => {
    console.log(`Relay #${index} listening on 0.0.0.0:${port} -> ${proxyUrl}`);
  });
}

for (let i = 1; i <= COUNT; i++) {
  createRelayInstance(i);
}

function getTunnelUrl(index) {
  try {
    const logPath = path.join(TUNNEL_LOG_DIR, `tunnel-${index}.log`);
    if (!fs.existsSync(logPath)) return null;
    const content = fs.readFileSync(logPath, "utf8");
    const matches = content.match(/https:\/\/[a-zA-Z0-9.-]+\.trycloudflare\.com/g);
    if (!matches || matches.length === 0) return null;
    return matches[matches.length - 1];
  } catch {
    return null;
  }
}

function createStatusServer() {
  const statusPort = BASE_PORT;
  const statusServer = http.createServer((req, res) => {
    const urlObj = new URL(req.url, `http://localhost:${statusPort}`);
    if (urlObj.pathname.startsWith("/log/")) {
      const idx = urlObj.pathname.replace("/log/", "").trim();
      const logPath = path.join(TUNNEL_LOG_DIR, `tunnel-${idx}.log`);
      if (fs.existsSync(logPath)) {
        res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
        return res.end(fs.readFileSync(logPath, "utf8"));
      }
      res.writeHead(404, { "content-type": "text/plain" });
      return res.end(`No log for tunnel ${idx}`);
    }

    if (urlObj.pathname === "/reset" || urlObj.pathname === "/rotate" || urlObj.pathname === "/api/reset") {
      const token = urlObj.searchParams.get("token") || req.headers["authorization"]?.replace(/^Bearer\s+/i, "");
      if (!token || token !== RESET_TOKEN) {
        res.writeHead(401, { "content-type": "application/json", "access-control-allow-origin": "*" });
        return res.end(JSON.stringify({
          status: "error",
          code: 401,
          error: "Unauthorized: invalid or missing 'token' parameter",
          usage: "GET /reset?token=<YOUR_TOKEN>[&relay=all|1..10][&tier=1|2]"
        }, null, 2));
      }

      if (isResetInProgress) {
        res.writeHead(429, { "content-type": "application/json", "access-control-allow-origin": "*" });
        return res.end(JSON.stringify({
          status: "error",
          code: 429,
          error: "A reset operation is already in progress. Please wait a moment."
        }, null, 2));
      }

      isResetInProgress = true;
      const t0 = Date.now();
      const targetRelay = (urlObj.searchParams.get("relay") || urlObj.searchParams.get("worker") || "all").toLowerCase();
      const tier = parseInt(urlObj.searchParams.get("tier") || "2", 10);

      const indices = [];
      if (targetRelay === "all") {
        for (let i = 1; i <= COUNT; i++) indices.push(i);
      } else {
        const idx = parseInt(targetRelay, 10);
        if (!isNaN(idx) && idx >= 1 && idx <= COUNT) {
          indices.push(idx);
        } else {
          isResetInProgress = false;
          res.writeHead(400, { "content-type": "application/json", "access-control-allow-origin": "*" });
          return res.end(JSON.stringify({
            status: "error",
            code: 400,
            error: `Invalid relay index: '${targetRelay}'. Must be 'all' or between 1 and ${COUNT}.`
          }, null, 2));
        }
      }

      (async () => {
        try {
          const containers = await findWarpContainers();
          if (!containers || containers.length === 0) {
            throw new Error("No WARP containers found or /var/run/docker.sock not accessible.");
          }

          const results = [];
          for (let i = 0; i < indices.length; i++) {
            const idx = indices[i];
            const oldIp = egressIps.get(idx) || null;
            const container = getWarpContainerForIndex(containers, idx);

            if (!container) {
              results.push({
                index: idx,
                status: "failed",
                error: `Container for warp-${idx} not found`,
                oldEgressIp: oldIp,
                newEgressIp: oldIp,
                tunnelUrl: getTunnelUrl(idx)
              });
              continue;
            }

            console.log(`[reset] Resetting relay #${idx} (${container.Names[0]}, tier ${tier})...`);

            if (tier >= 2) {
              await execInContainer(container.Id, ["rm", "-f", "/etc/wireguard/wg0.conf"]);
            }

            await restartContainer(container.Id);

            if (i < indices.length - 1) {
              await new Promise(r => setTimeout(r, 1500));
            }

            results.push({
              index: idx,
              container: container.Names[0].replace(/^\//, ""),
              oldEgressIp: oldIp,
              tunnelUrl: getTunnelUrl(idx),
              status: "rotated"
            });
          }

          // Wait for each rotated container to settle and capture fresh egress IP
          for (const item of results) {
            if (item.status === "rotated") {
              item.newEgressIp = await waitForNewEgress(item.index, 18);
              item.status = item.newEgressIp ? "ok" : "slow_startup";
            }
          }

          const payload = {
            status: "ok",
            action: "reset",
            tier,
            count: results.length,
            elapsedMs: Date.now() - t0,
            relays: results
          };

          res.writeHead(200, {
            "content-type": "application/json",
            "access-control-allow-origin": "*"
          });
          res.end(JSON.stringify(payload, null, 2));
        } catch (err) {
          console.error("[reset] Error during reset operation:", err);
          res.writeHead(500, { "content-type": "application/json", "access-control-allow-origin": "*" });
          res.end(JSON.stringify({
            status: "error",
            code: 500,
            error: err.message || String(err)
          }, null, 2));
        } finally {
          isResetInProgress = false;
        }
      })();
      return;
    }

    const tunnels = [];
    for (let i = 1; i <= COUNT; i++) {
      const url = getTunnelUrl(i);
      tunnels.push({
        index: i,
        port: BASE_PORT + i,
        egressIp: egressIps.get(i) || null,
        tunnelUrl: url,
        command: url ? `/freeflow add ${url} relay-${i}` : null,
      });
    }

    const payload = {
      status: "ok",
      count: COUNT,
      basePort: BASE_PORT,
      tunnels,
    };

    res.writeHead(200, {
      "content-type": "application/json",
      "access-control-allow-origin": "*",
    });
    res.end(JSON.stringify(payload, null, 2));
  });

  statusServer.listen(statusPort, "0.0.0.0", () => {
    console.log(`Status server listening on 0.0.0.0:${statusPort}`);
  });

  const knownUrls = new Map();
  const pollTunnels = async () => {
    let updated = false;
    for (let i = 1; i <= COUNT; i++) {
      const url = getTunnelUrl(i);
      if (url && url !== knownUrls.get(i)) {
        knownUrls.set(i, url);
        updated = true;
        console.log(`[relay] Tunnel #${i} ready: ${url} -> /freeflow add ${url} relay-${i}`);
      }
      probeEgress(i);
    }
    if (updated) {
      console.log(`[relay] Active tunnels (${knownUrls.size}/${COUNT}):`);
      for (const [idx, u] of knownUrls.entries()) {
        console.log(`  - Relay #${idx} (${egressIps.get(idx) || "detecting IP"}): ${u}`);
      }
    }
  };

  const timer = setInterval(pollTunnels, 3000);
  if (timer.unref) timer.unref();
  pollTunnels();
}

createStatusServer();

