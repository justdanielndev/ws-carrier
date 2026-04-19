import { WebSocket } from "ws";
import { randomBytes } from "node:crypto";
import { ID_BITS, K, MAX_ID, PeerRef, RoutingTable, normalizeIdHex } from "./routing.js";
import { hexToBigInt } from "./xor.js";

const MAX_FRAME_BYTES = 2048;
const HELLO_TIMEOUT_MS = 5_000;
const HEARTBEAT_INTERVAL_MS = 30_000;
const HEARTBEAT_GRACE_MS = 10_000;
const RECONNECT_BASE_MS = 2_000;
const RECONNECT_MAX_MS = 60_000;
const FIND_NODE_LIMIT_MAX = 20;
const REFRESH_INTERVAL_MS = 60_000;
const DHT_QUERY_TIMEOUT_MS = 3_000;

export interface PeerHandlers {
  myServerId: string;
  myServerAddr: string | null;
  localNodeIds(): string[];
  onPeerForward(fromServerId: string, dst: string, bytes: string): void;
  onPeerLost?(serverId: string): void;
  log(msg: string): void;
}

export interface Peer extends PeerRef {
  ws: WebSocket;
  inbound: boolean;
}

interface PeerInternal extends Peer {
  ready: boolean;
  heartbeat?: NodeJS.Timeout;
  lastSeen: number;
}

export interface DhtQueryResult {
  targetId: string;
  found: boolean;
  serverId?: string;
  serverAddr?: string | null;
  closest: PeerRef[];
}

interface PendingQuery {
  resolve(r: DhtQueryResult): void;
  reject(e: Error): void;
  timer: NodeJS.Timeout;
}

export class PeerManager {
  private readonly table: RoutingTable<PeerInternal>;
  private readonly dialing = new Set<string>();
  private readonly known = new Set<string>();
  private readonly backoff = new Map<string, number>();
  private readonly pending = new Map<string, PendingQuery>();
  private refreshTimer?: NodeJS.Timeout;

  constructor(private readonly h: PeerHandlers) {
    this.table = new RoutingTable(h.myServerId);
  }

  start(): void {
    if (this.refreshTimer) return;
    this.refreshTimer = setInterval(() => this.refreshHungryBuckets(), REFRESH_INTERVAL_MS);
  }

  stop(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = undefined;
    }
    for (const p of this.pending.values()) {
      clearTimeout(p.timer);
      p.reject(new Error("shutdown"));
    }
    this.pending.clear();
  }

  get routingTable(): RoutingTable<PeerInternal> {
    return this.table;
  }

  get entries(): Iterable<Peer> {
    return this.table.all();
  }

  getByServerId(serverId: string): Peer | undefined {
    return this.table.get(serverId);
  }

  sendTo(serverId: string, obj: unknown): boolean {
    const p = this.table.get(serverId);
    if (!p || p.ws.readyState !== WebSocket.OPEN) return false;
    try {
      p.ws.send(JSON.stringify(obj));
      return true;
    } catch {
      return false;
    }
  }

  closestPeers(targetHex: string, n: number): Peer[] {
    return this.table.closestTo(targetHex, n);
  }

  dhtQuery(peerServerId: string, targetIdHex: string, timeoutMs = DHT_QUERY_TIMEOUT_MS): Promise<DhtQueryResult> {
    return new Promise((resolve, reject) => {
      const requestId = randomBytes(8).toString("hex");
      const timer = setTimeout(() => {
        if (this.pending.delete(requestId)) reject(new Error("dht_query timeout"));
      }, timeoutMs);
      this.pending.set(requestId, { resolve, reject, timer });
      const ok = this.sendTo(peerServerId, {
        type: "dht_query",
        targetId: targetIdHex,
        requestId,
      });
      if (!ok) {
        this.pending.delete(requestId);
        clearTimeout(timer);
        reject(new Error("peer not connected"));
      }
    });
  }

  adoptInbound(ws: WebSocket, hello: { serverId: string; serverAddr: string | null }): void {
    const theirId = normalizeIdHex(hello.serverId);
    if (!theirId) {
      try { ws.close(4014, "bad server id"); } catch {}
      return;
    }
    if (theirId === this.h.myServerId) {
      try { ws.close(4010, "loopback"); } catch {}
      return;
    }
    if (this.table.get(theirId)) {
      try { ws.close(4011, "duplicate peer"); } catch {}
      return;
    }

    const peer: PeerInternal = {
      serverId: theirId,
      addr: isDialable(hello.serverAddr) ? hello.serverAddr : null,
      ws,
      inbound: true,
      ready: false,
      lastSeen: Date.now(),
    };

    const outcome = this.table.add(peer);
    if (outcome.result === "rejected") {
      try { ws.close(4016, "routing table full"); } catch {}
      return;
    }
    if (outcome.evicted) {
      this.h.log(`evict peer ${outcome.evicted.serverId} (farther than new ${theirId})`);
      try { outcome.evicted.ws.close(4017, "evicted by closer peer"); } catch {}
    }

    try {
      ws.send(
        JSON.stringify({
          type: "server_hello_ack",
          serverId: this.h.myServerId,
          serverAddr: this.h.myServerAddr,
        }),
      );
    } catch {}
    this.attachSocket(peer);
    this.markReady(peer);
  }

  dial(addr: string): void {
    if (!isDialable(addr)) return;
    const key = normalizeAddr(addr);
    if (this.known.has(key)) return;
    this.known.add(key);
    this.connect(addr);
  }

  private connect(addr: string): void {
    const key = normalizeAddr(addr);
    if (this.dialing.has(key)) return;
    this.dialing.add(key);
    const attempt = (this.backoff.get(key) ?? 0) + 1;
    this.backoff.set(key, attempt);
    this.h.log(`dial peer ${addr} (attempt ${attempt})`);

    let ws: WebSocket;
    try {
      ws = new WebSocket(addr, { maxPayload: MAX_FRAME_BYTES });
    } catch (err) {
      this.h.log(`dial error ${addr}: ${(err as Error).message}`);
      this.dialing.delete(key);
      this.scheduleReconnect(addr);
      return;
    }

    const helloTimer = setTimeout(() => {
      try { ws.close(4012, "hello timeout"); } catch {}
    }, HELLO_TIMEOUT_MS);

    ws.on("open", () => {
      try {
        ws.send(
          JSON.stringify({
            type: "server_hello",
            serverId: this.h.myServerId,
            serverAddr: this.h.myServerAddr,
          }),
        );
      } catch {}
    });

    const onHelloAck = (raw: WebSocket.RawData) => {
      let msg: any;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      if (!msg || msg.type !== "server_hello_ack" || typeof msg.serverId !== "string") return;
      clearTimeout(helloTimer);
      ws.removeListener("message", onHelloAck);
      this.dialing.delete(key);

      const theirId = normalizeIdHex(msg.serverId);
      if (!theirId || theirId === this.h.myServerId) {
        try { ws.close(4010, "loopback"); } catch {}
        return;
      }
      if (this.table.get(theirId)) {
        try { ws.close(4011, "duplicate peer"); } catch {}
        return;
      }

      const peer: PeerInternal = {
        serverId: theirId,
        addr,
        ws,
        inbound: false,
        ready: false,
        lastSeen: Date.now(),
      };
      const outcome = this.table.add(peer);
      if (outcome.result === "rejected") {
        this.h.log(`reject outbound peer ${theirId} — bucket full and farther`);
        try { ws.close(4016, "routing table full"); } catch {}
        return;
      }
      if (outcome.evicted) {
        this.h.log(`evict peer ${outcome.evicted.serverId} (farther than new ${theirId})`);
        try { outcome.evicted.ws.close(4017, "evicted by closer peer"); } catch {}
      }
      this.backoff.set(key, 0);
      this.attachSocket(peer);
      this.markReady(peer);
    };
    ws.on("message", onHelloAck);

    ws.on("close", () => {
      clearTimeout(helloTimer);
      this.dialing.delete(key);
      this.scheduleReconnect(addr);
    });
    ws.on("error", (err) => this.h.log(`peer ws error ${addr}: ${err.message}`));
  }

  private scheduleReconnect(addr: string): void {
    const key = normalizeAddr(addr);
    const attempt = this.backoff.get(key) ?? 1;
    const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** Math.max(0, attempt - 1));
    setTimeout(() => {
      if (this.dialing.has(key)) return;
      this.connect(addr);
    }, delay);
  }

  private attachSocket(peer: PeerInternal): void {
    peer.heartbeat = setInterval(() => {
      if (peer.ws.readyState !== WebSocket.OPEN) return;
      const since = Date.now() - peer.lastSeen;
      if (since > HEARTBEAT_INTERVAL_MS + HEARTBEAT_GRACE_MS) {
        this.h.log(`peer ${peer.serverId} heartbeat timeout (idle ${since}ms)`);
        try { peer.ws.close(4013, "heartbeat timeout"); } catch {}
        return;
      }
      try { peer.ws.send(JSON.stringify({ type: "ping" })); } catch {}
    }, HEARTBEAT_INTERVAL_MS);

    peer.ws.on("message", (raw) => this.handlePeerMessage(peer, raw));
    peer.ws.on("close", () => {
      if (peer.heartbeat) clearInterval(peer.heartbeat);
      if (this.table.get(peer.serverId) === peer) this.table.remove(peer.serverId);
      this.h.onPeerLost?.(peer.serverId);
      this.h.log(`peer ${peer.serverId} disconnected`);
    });
  }

  private markReady(peer: PeerInternal): void {
    peer.ready = true;
    this.h.log(
      `peer ${peer.serverId} ready (${peer.inbound ? "inbound" : "outbound"}, addr=${peer.addr ?? "?"}, table=${this.table.size()}/${ID_BITS * K})`,
    );
    this.sendTo(peer.serverId, { type: "find_node", targetId: this.h.myServerId, limit: K * 4 });
  }

  private handlePeerMessage(peer: PeerInternal, raw: WebSocket.RawData): void {
    peer.lastSeen = Date.now();
    let msg: any;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== "object") return;

    switch (msg.type) {
      case "ping":
        return;
      case "peer_forward":
        if (typeof msg.dst === "string" && typeof msg.bytes === "string") {
          this.h.onPeerForward(peer.serverId, msg.dst.toLowerCase(), msg.bytes);
        }
        return;
      case "find_node":
        return this.handleFindNode(peer, msg);
      case "find_node_response":
        return this.handleFindNodeResponse(msg);
      case "dht_query":
        return this.handleIncomingDhtQuery(peer, msg);
      case "dht_response":
        return this.handleDhtResponse(msg);
      default:
        return;
    }
  }

  private handleFindNode(peer: PeerInternal, msg: any): void {
    const target = typeof msg.targetId === "string" ? normalizeIdHex(msg.targetId) : null;
    if (!target) return;
    const rawLimit = typeof msg.limit === "number" ? msg.limit : K * 4;
    const limit = Math.max(1, Math.min(FIND_NODE_LIMIT_MAX, Math.floor(rawLimit)));

    const candidates: PeerRef[] = [];
    if (isDialable(this.h.myServerAddr)) {
      candidates.push({ serverId: this.h.myServerId, addr: this.h.myServerAddr });
    }
    for (const p of this.table.all()) {
      if (p.serverId === peer.serverId) continue;
      if (!isDialable(p.addr)) continue;
      candidates.push({ serverId: p.serverId, addr: p.addr });
    }
    const t = hexToBigInt(target);
    candidates.sort((a, b) => {
      const da = hexToBigInt(a.serverId) ^ t;
      const db = hexToBigInt(b.serverId) ^ t;
      return da < db ? -1 : da > db ? 1 : 0;
    });
    try {
      peer.ws.send(
        JSON.stringify({ type: "find_node_response", targetId: target, peers: candidates.slice(0, limit) }),
      );
    } catch {}
  }

  private handleFindNodeResponse(msg: any): void {
    if (!Array.isArray(msg.peers)) return;
    for (const entry of msg.peers) {
      if (!entry || typeof entry !== "object") continue;
      const id = typeof entry.serverId === "string" ? normalizeIdHex(entry.serverId) : null;
      const addr = typeof entry.addr === "string"
        ? entry.addr
        : typeof entry.serverAddr === "string" ? entry.serverAddr : null;
      if (!id || id === this.h.myServerId) continue;
      if (!isDialable(addr)) continue;
      if (this.table.get(id)) continue;
      if (this.wouldAccept(id)) this.dial(addr);
    }
  }

  private handleIncomingDhtQuery(peer: PeerInternal, msg: any): void {
    const target = typeof msg.targetId === "string" ? normalizeIdHex(msg.targetId) : null;
    if (!target) return;
    const requestId = typeof msg.requestId === "string" ? msg.requestId : undefined;

    const haveLocal = this.h.localNodeIds().includes(target);

    const reply: any = {
      type: "dht_response",
      targetId: target,
      found: haveLocal,
    };
    if (requestId) reply.requestId = requestId;

    if (haveLocal) {
      reply.serverId = this.h.myServerId;
      reply.serverAddr = isDialable(this.h.myServerAddr) ? this.h.myServerAddr : null;
    } else {
      reply.closest = this.table
        .closestTo(target, K)
        .filter((p) => isDialable(p.addr))
        .map((p) => ({ serverId: p.serverId, addr: p.addr }));
    }
    try { peer.ws.send(JSON.stringify(reply)); } catch {}
  }

  private handleDhtResponse(msg: any): void {
    const requestId = typeof msg.requestId === "string" ? msg.requestId : null;
    if (!requestId) return;
    const pending = this.pending.get(requestId);
    if (!pending) return;
    this.pending.delete(requestId);
    clearTimeout(pending.timer);

    const closestRaw = Array.isArray(msg.closest) ? msg.closest : [];
    const closest: PeerRef[] = [];
    for (const c of closestRaw) {
      const id = typeof c?.serverId === "string" ? normalizeIdHex(c.serverId) : null;
      const addr = typeof c?.addr === "string" ? c.addr : typeof c?.serverAddr === "string" ? c.serverAddr : null;
      if (!id || !isDialable(addr)) continue;
      closest.push({ serverId: id, addr });
    }

    pending.resolve({
      targetId: typeof msg.targetId === "string" ? msg.targetId.toLowerCase() : "",
      found: Boolean(msg.found),
      serverId: typeof msg.serverId === "string" ? msg.serverId.toLowerCase() : undefined,
      serverAddr: typeof msg.serverAddr === "string" ? msg.serverAddr : null,
      closest,
    });
  }

  private refreshHungryBuckets(): void {
    if (this.table.size() === 0) return;
    const hungry = this.table.hungryBuckets();
    if (hungry.length === 0) return;

    const self = hexToBigInt(this.h.myServerId);
    for (const i of hungry) {
      const low = 1n << BigInt(i);
      const highExclusive = i + 1 >= ID_BITS ? MAX_ID + 1n : 1n << BigInt(i + 1);
      const range = highExclusive - low;
      const offset = BigInt(Math.floor(Math.random() * Number(range)));
      const distance = low + offset;
      const target = self ^ distance;
      const targetHex = target.toString(16).padStart(12, "0");
      const closest = this.table.closestTo(targetHex, 1)[0];
      if (!closest) continue;
      this.sendTo(closest.serverId, { type: "find_node", targetId: targetHex, limit: K * 2 });
    }
  }

  private wouldAccept(theirIdHex: string): boolean {
    const self = hexToBigInt(this.h.myServerId);
    const their = hexToBigInt(theirIdHex);
    const d = self ^ their;
    if (d === 0n) return false;
    let bucketIdx = 0;
    for (let dd = d; dd > 1n; dd >>= 1n) bucketIdx++;
    const bucket = this.table.snapshot().find((b) => b.bucket === bucketIdx);
    if (!bucket || bucket.peers.length < K) return true;
    let farthest = self ^ hexToBigInt(bucket.peers[0].serverId);
    for (let i = 1; i < bucket.peers.length; i++) {
      const dd = self ^ hexToBigInt(bucket.peers[i].serverId);
      if (dd > farthest) farthest = dd;
    }
    return d < farthest;
  }
}

function isDialable(addr: string | null | undefined): addr is string {
  return typeof addr === "string" && /^wss?:\/\//i.test(addr);
}

function normalizeAddr(a: string): string {
  return a.replace(/\/+$/, "").toLowerCase();
}
