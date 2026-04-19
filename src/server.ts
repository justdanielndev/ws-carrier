import { WebSocketServer, WebSocket } from "ws";
import { createServer } from "node:http";
import { randomBytes } from "node:crypto";
import { PeerManager } from "./peers.js";
import { loadBootstrapPeers } from "./bootstrap.js";
import { generate44BitHexId, ID_BITS, K, MAX_ID, normalizeIdHex } from "./routing.js";

interface TransportCaps {
  type?: number;
  bandwidth?: number;
  latency?: "low" | "medium" | "high";
  broadcast?: boolean;
  reliable?: boolean;
}

interface RegisteredWs extends WebSocket {
  nodeId?: string;
  isPeer?: boolean;
  transportCaps?: TransportCaps;
  coverageArea?: unknown;
  lastSeen?: number;
  tokens?: number;
  lastTokenAt?: number;
  registerDeadline?: NodeJS.Timeout;
  heartbeat?: NodeJS.Timeout;
}

interface RegisterMsg {
  type: "register";
  nodeId: string;
  transportCaps?: TransportCaps;
  coverageArea?: unknown;
}
interface ForwardMsg { type: "forward"; dst: string; bytes: string }
interface DhtQueryMsg { type: "dht_query"; targetId: string }
interface PingMsg { type: "ping" }
interface ServerHelloMsg { type: "server_hello"; serverId: string; serverAddr: string | null }
type ClientMsg = RegisterMsg | ForwardMsg | DhtQueryMsg | PingMsg | ServerHelloMsg;

const PORT = Number(process.env.PORT ?? 8080);
const REGISTER_TIMEOUT_MS = 5_000;
const HEARTBEAT_INTERVAL_MS = 30_000;
const HEARTBEAT_GRACE_MS = 10_000;
const RATE_MAX_PER_SEC = Number(process.env.RATE_MAX_PER_SEC ?? 100);
const RATE_BURST = Number(process.env.RATE_BURST ?? 100);
const MAX_PAYLOAD_BYTES = 1024;
export const MAX_FRAME_BYTES = 2048;

const { id: SERVER_ID, generated: SERVER_ID_EPHEMERAL } = resolveServerId(process.env.SERVER_ID);
const SERVER_ADDR = process.env.SERVER_ADDR ?? null;

function die(msg: string): never {
  console.error(`[znp-carrier] fatal: ${msg}`);
  process.exit(1);
}

function genCmd(): string {
  return `node -e 'const b=require("crypto").randomBytes(6);b[0]&=0x0f;console.log(b.toString("hex"))'`;
}

function resolveServerId(raw: string | undefined): { id: string; generated: boolean } {
  if (raw === undefined || raw === "") {
    return { id: generate44BitHexId(randomBytes(6)), generated: true };
  }

  let s = raw.trim().toLowerCase();
  if (s.startsWith("0x")) s = s.slice(2);

  if (s.length === 0) {
    die(`SERVER_ID is empty. Unset the variable to auto-generate, or provide a value. Generate one with:\n  ${genCmd()}`);
  }
  if (!/^[0-9a-f]+$/.test(s)) {
    die(`SERVER_ID must contain only hex characters [0-9a-f]. Got: "${raw}"`);
  }
  if (s.length > 12) {
    die(`SERVER_ID must be at most 12 hex characters (44-bit id). Got ${s.length} chars: "${s}"`);
  }

  const n = BigInt("0x" + s);

  if (n > MAX_ID) {
    const low44 = n & MAX_ID;
    const low44Hex = low44.toString(16).padStart(12, "0");
    const lowReserved = low44 === 0n || low44 === MAX_ID;
    const fixes = lowReserved
      ? `  • Generate a fresh valid id:\n      ${genCmd()}`
      : `  • Use the low ${ID_BITS} bits: SERVER_ID=${low44Hex}\n` +
        `  • Or generate a fresh valid id:\n      ${genCmd()}`;
    die(
      `SERVER_ID ${s} has bits set above the ${ID_BITS}-bit keyspace ` +
      `(top nibble must be 0). Fix:\n${fixes}`,
    );
  }
  if (n === 0n) {
    die(`SERVER_ID=0 is reserved (indistinguishable from unset). Pick any other value:\n  ${genCmd()}`);
  }
  if (n === MAX_ID) {
    die(`SERVER_ID ${s} collides with the broadcast sentinel 0x${MAX_ID.toString(16)}. Pick any other value:\n  ${genCmd()}`);
  }

  return { id: n.toString(16).padStart(12, "0"), generated: false };
}

const registry = new Map<string, RegisteredWs>();

function log(msg: string): void {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

const peers = new PeerManager({
  myServerId: SERVER_ID,
  myServerAddr: SERVER_ADDR,
  log,
  localNodeIds: () => Array.from(registry.keys()),
  onPeerForward: (_fromServerId, dst, bytes) => {
    const target = registry.get(dst);
    if (!target || target.readyState !== WebSocket.OPEN) return;
    try { target.send(JSON.stringify({ bytes })); } catch {}
  },
});
peers.start();

function takeToken(ws: RegisteredWs): boolean {
  const now = Date.now();
  if (ws.tokens === undefined) {
    ws.tokens = RATE_BURST;
    ws.lastTokenAt = now;
  }
  const elapsed = (now - (ws.lastTokenAt ?? now)) / 1000;
  ws.tokens = Math.min(RATE_BURST, (ws.tokens ?? 0) + elapsed * RATE_MAX_PER_SEC);
  ws.lastTokenAt = now;
  if (ws.tokens < 1) return false;
  ws.tokens -= 1;
  return true;
}

function isValidNodeId(v: unknown): v is string {
  return typeof v === "string" && /^[0-9a-f]{1,12}$/i.test(v);
}

function sendObj(ws: WebSocket, obj: unknown): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  try { ws.send(JSON.stringify(obj)); } catch {}
}

function sendError(ws: WebSocket, code: string, message: string): void {
  sendObj(ws, { type: "error", code, message });
}

function base64ByteLength(b64: string): number {
  const len = b64.length;
  if (len === 0) return 0;
  let pad = 0;
  if (b64.endsWith("==")) pad = 2;
  else if (b64.endsWith("=")) pad = 1;
  return Math.floor((len * 3) / 4) - pad;
}

function rawByteLength(raw: WebSocket.RawData): number {
  if (Buffer.isBuffer(raw)) return raw.length;
  if (Array.isArray(raw)) {
    let n = 0;
    for (const b of raw) n += b.length;
    return n;
  }
  if (raw instanceof ArrayBuffer) return raw.byteLength;
  return 0;
}

function handleClientMessage(ws: RegisteredWs, raw: WebSocket.RawData): void {
  ws.lastSeen = Date.now();

  if (rawByteLength(raw) > MAX_FRAME_BYTES) {
    try { ws.close(4015, "frame too large"); } catch {}
    return;
  }

  let msg: ClientMsg;
  try { msg = JSON.parse(raw.toString()) as ClientMsg; } catch { return; }
  if (!msg || typeof msg !== "object") return;

  if (!ws.nodeId && !ws.isPeer && (msg as ServerHelloMsg).type === "server_hello") {
    return handleInboundServerHello(ws, msg as ServerHelloMsg);
  }

  if (!takeToken(ws)) return;

  switch (msg.type) {
    case "ping": return;
    case "register": return handleRegister(ws, msg);
    case "forward":
      handleForward(ws, msg).catch((err) => log(`forward error: ${(err as Error).message}`));
      return;
    case "dht_query": return handleDhtQuery(ws, msg);
    default: sendError(ws, "unknown_type", "unknown message type");
  }
}

function handleInboundServerHello(ws: RegisteredWs, msg: ServerHelloMsg): void {
  if (typeof msg.serverId !== "string") {
    try { ws.close(4014, "bad hello"); } catch {}
    return;
  }
  const theirId = msg.serverId.toLowerCase();
  ws.isPeer = true;
  if (ws.registerDeadline) {
    clearTimeout(ws.registerDeadline);
    ws.registerDeadline = undefined;
  }
  if (ws.heartbeat) {
    clearInterval(ws.heartbeat);
    ws.heartbeat = undefined;
  }
  ws.removeAllListeners("message");
  ws.removeAllListeners("close");
  peers.adoptInbound(ws, { serverId: theirId, serverAddr: typeof msg.serverAddr === "string" ? msg.serverAddr : null });
}

function handleRegister(ws: RegisteredWs, msg: RegisterMsg): void {
  if (!isValidNodeId(msg.nodeId)) {
    sendError(ws, "bad_node_id", "nodeId must be 1–12 hex chars");
    return;
  }
  const nodeId = msg.nodeId.toLowerCase();

  const existing = registry.get(nodeId);
  if (existing && existing !== ws) {
    try { existing.close(4000, "replaced by new connection"); } catch {}
  }

  ws.nodeId = nodeId;
  ws.transportCaps = msg.transportCaps;
  ws.coverageArea = msg.coverageArea;
  registry.set(nodeId, ws);

  if (ws.registerDeadline) {
    clearTimeout(ws.registerDeadline);
    ws.registerDeadline = undefined;
  }

  log(`register ${nodeId} (local=${registry.size})`);
  sendObj(ws, { type: "registered", nodeId, serverId: SERVER_ID });
}

const MAX_DHT_HOPS = 5;

async function lookupViaDht(target: string): Promise<{ serverId: string; serverAddr: string | null } | null> {
  const asked = new Set<string>();
  let frontier = peers.closestPeers(target, K);
  for (let hop = 0; hop < MAX_DHT_HOPS; hop++) {
    if (frontier.length === 0) return null;
    for (const p of frontier) asked.add(p.serverId);

    const results = await Promise.allSettled(
      frontier.map((p) => peers.dhtQuery(p.serverId, target)),
    );

    const hints: { serverId: string; addr: string }[] = [];
    for (const r of results) {
      if (r.status !== "fulfilled") continue;
      const v = r.value;
      if (v.found && v.serverId) {
        return { serverId: v.serverId, serverAddr: v.serverAddr ?? null };
      }
      for (const c of v.closest) {
        if (asked.has(c.serverId)) continue;
        if (c.addr) hints.push({ serverId: c.serverId, addr: c.addr });
      }
    }
    const seenIds = new Set<string>();
    const connected = hints
      .filter((h) => !seenIds.has(h.serverId) && seenIds.add(h.serverId))
      .map((h) => peers.getByServerId(h.serverId))
      .filter((p): p is NonNullable<typeof p> => Boolean(p) && !asked.has(p!.serverId));
    const t = BigInt("0x" + target);
    connected.sort((a, b) => {
      const da = BigInt("0x" + a.serverId) ^ t;
      const db = BigInt("0x" + b.serverId) ^ t;
      return da < db ? -1 : da > db ? 1 : 0;
    });
    frontier = connected.slice(0, K);
  }
  return null;
}

async function handleForward(ws: RegisteredWs, msg: ForwardMsg): Promise<void> {
  if (!ws.nodeId) {
    sendError(ws, "not_registered", "register before forwarding");
    return;
  }
  if (!isValidNodeId(msg.dst)) {
    sendError(ws, "bad_dst", "dst must be 1–12 hex chars");
    return;
  }
  if (typeof msg.bytes !== "string" || !/^[A-Za-z0-9+/]+=*$/.test(msg.bytes)) {
    sendError(ws, "bad_bytes", "bytes must be base64");
    return;
  }
  if (base64ByteLength(msg.bytes) > MAX_PAYLOAD_BYTES) {
    sendError(ws, "payload_too_large", `max ${MAX_PAYLOAD_BYTES} bytes`);
    return;
  }

  const dst = msg.dst.toLowerCase();

  if (dst === "ffffffffffff") {
    sendError(ws, "dst_not_found", "broadcast is not routed by ws-carriers");
    return;
  }

  const local = registry.get(dst);
  if (local && local.readyState === WebSocket.OPEN) {
    local.send(JSON.stringify({ bytes: msg.bytes }));
    sendObj(ws, { type: "forwarded", dst, status: "delivered" });
    return;
  }

  const found = await lookupViaDht(dst);
  if (!found) {
    sendError(ws, "dst_not_found", `${dst} not reachable within ${MAX_DHT_HOPS} DHT hops`);
    return;
  }
  if (peers.sendTo(found.serverId, { type: "peer_forward", dst, bytes: msg.bytes })) {
    sendObj(ws, { type: "forwarded", dst, status: "relayed", via: found.serverId });
    return;
  }
  sendError(ws, "dst_not_found", `located at ${found.serverId} but no active connection`);
}

function handleDhtQuery(ws: RegisteredWs, msg: DhtQueryMsg & { requestId?: string }): void {
  if (!isValidNodeId(msg.targetId)) {
    sendError(ws, "bad_target_id", "targetId must be 1–12 hex chars");
    return;
  }
  const target = msg.targetId.toLowerCase();
  const requestId = typeof msg.requestId === "string" ? msg.requestId : undefined;

  if (registry.has(target)) {
    const reply: Record<string, unknown> = {
      type: "dht_response",
      targetId: target,
      found: true,
      serverId: SERVER_ID,
      serverAddr: isDialable(SERVER_ADDR) ? SERVER_ADDR : null,
    };
    if (requestId) reply.requestId = requestId;
    sendObj(ws, reply);
    return;
  }

  const closest = peers
    .closestPeers(target, K)
    .filter((p) => isDialable(p.addr))
    .map((p) => ({ serverId: p.serverId, addr: p.addr }));
  const reply: Record<string, unknown> = {
    type: "dht_response",
    targetId: target,
    found: false,
    closest,
  };
  if (requestId) reply.requestId = requestId;
  sendObj(ws, reply);
}

function isDialable(addr: string | null | undefined): addr is string {
  return typeof addr === "string" && /^wss?:\/\//i.test(addr);
}

function startHeartbeat(ws: RegisteredWs): void {
  ws.heartbeat = setInterval(() => {
    if (ws.readyState !== WebSocket.OPEN) return;
    const since = Date.now() - (ws.lastSeen ?? 0);
    if (since > HEARTBEAT_INTERVAL_MS + HEARTBEAT_GRACE_MS) {
      log(`heartbeat timeout ${ws.nodeId ?? "?"} (idle ${since}ms)`);
      try { ws.close(4001, "heartbeat timeout"); } catch {}
      return;
    }
    sendObj(ws, { type: "ping" });
  }, HEARTBEAT_INTERVAL_MS);
}

const httpServer = createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        service: "znp-ws-carrier",
        carrierId: SERVER_ID,
        serverId: SERVER_ID,
        localNodes: registry.size,
        routingTable: {
          size: peers.routingTable.size(),
          capacity: 44 * K,
          buckets: peers.routingTable.snapshot().map((b) => ({
            bucket: b.bucket,
            peers: b.peers.map((p) => ({ serverId: p.serverId, addr: p.addr })),
          })),
        },
      }),
    );
    return;
  }
  res.writeHead(404, { "content-type": "application/json" });
  res.end(JSON.stringify({ ok: false }));
});

const wss = new WebSocketServer({ server: httpServer, maxPayload: MAX_FRAME_BYTES });

wss.on("connection", (rawWs) => {
  const ws = rawWs as RegisteredWs;
  ws.lastSeen = Date.now();

  ws.registerDeadline = setTimeout(() => {
    if (!ws.nodeId && !ws.isPeer) {
      log("no register/hello within timeout — closing");
      try { ws.close(4002, "register timeout"); } catch {}
    }
  }, REGISTER_TIMEOUT_MS);

  startHeartbeat(ws);

  ws.on("message", (raw) => handleClientMessage(ws, raw));

  ws.on("close", () => {
    if (ws.registerDeadline) clearTimeout(ws.registerDeadline);
    if (ws.heartbeat) clearInterval(ws.heartbeat);
    if (ws.nodeId && registry.get(ws.nodeId) === ws) {
      const nodeId = ws.nodeId;
      registry.delete(nodeId);
      log(`disconnect ${nodeId} (local=${registry.size})`);
    }
  });

  ws.on("error", (err) => log(`ws error: ${err.message}`));
});

httpServer.listen(PORT, async () => {
  log(`znp ws-carrier listening on :${PORT} carrierId=${SERVER_ID}${SERVER_ADDR ? ` addr=${SERVER_ADDR}` : ""}`);

  const envPeers = (process.env.BOOTSTRAP_SERVERS ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const bootstrap = await loadBootstrapPeers({
    bootstrapUrl: process.env.BOOTSTRAP_URL,
    bootstrapServers: envPeers.length ? envPeers : undefined,
    selfAddr: SERVER_ADDR,
    log,
  });
  if (bootstrap.length === 0) {
    log("no bootstrap peers (single-carrier mode)");
    return;
  }
  if (!SERVER_ADDR) {
    log(
      "WARNING: SERVER_ADDR not set. Peers will accept this carrier's DHT messages but clients that dht_query other carriers will not receive a dialable addr for this carrier. Set SERVER_ADDR=wss://... to participate as a routing destination.",
    );
  }
  if (SERVER_ID_EPHEMERAL) {
    log(
      `WARNING: SERVER_ID not set — using ephemeral ${SERVER_ID}. After every restart peers will see this carrier as a new node, reshuffling routing tables and breaking in-flight peer_forwards. Set SERVER_ID=<12 hex chars, top nibble 0> to make identity stable. Generate: ${genCmd()}`,
    );
  }
  log(`dialing ${bootstrap.length} bootstrap peer(s)`);
  for (const addr of bootstrap) peers.dial(addr);
});
