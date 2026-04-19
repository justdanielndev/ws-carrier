import { hexToBigInt, xorDistance } from "./xor.js";

export const ID_BITS = 44;
export const K = 3;
export const MAX_ID = (1n << BigInt(ID_BITS)) - 1n;

export interface PeerRef {
  serverId: string;
  addr: string | null;
}

export function bucketIndex(distance: bigint): number {
  if (distance <= 0n) return -1;
  let i = 0;
  let d = distance;
  while (d > 1n) {
    d >>= 1n;
    i++;
  }
  return i;
}

export class RoutingTable<T extends PeerRef> {
  private readonly buckets: T[][];
  private readonly selfId: bigint;

  constructor(private readonly selfIdHex: string) {
    this.selfId = hexToBigInt(selfIdHex);
    this.buckets = Array.from({ length: ID_BITS }, () => [] as T[]);
  }

  add(peer: T): { result: "added" | "updated" | "rejected"; evicted?: T } {
    if (peer.serverId === this.selfIdHex) return { result: "rejected" };
    const their = hexToBigInt(peer.serverId);
    const d = this.selfId ^ their;
    const idx = bucketIndex(d);
    if (idx < 0 || idx >= ID_BITS) return { result: "rejected" };

    const bucket = this.buckets[idx];
    const existing = bucket.findIndex((p) => p.serverId === peer.serverId);
    if (existing >= 0) {
      bucket.splice(existing, 1);
      bucket.push(peer);
      return { result: "updated" };
    }
    if (bucket.length < K) {
      bucket.push(peer);
      return { result: "added" };
    }
    let farthestAt = 0;
    let farthestDist = this.selfId ^ hexToBigInt(bucket[0].serverId);
    for (let i = 1; i < bucket.length; i++) {
      const dd = this.selfId ^ hexToBigInt(bucket[i].serverId);
      if (dd > farthestDist) {
        farthestDist = dd;
        farthestAt = i;
      }
    }
    if (d < farthestDist) {
      const [evicted] = bucket.splice(farthestAt, 1);
      bucket.push(peer);
      return { result: "added", evicted };
    }
    return { result: "rejected" };
  }

  remove(serverId: string): boolean {
    for (const bucket of this.buckets) {
      const i = bucket.findIndex((p) => p.serverId === serverId);
      if (i >= 0) {
        bucket.splice(i, 1);
        return true;
      }
    }
    return false;
  }

  get(serverId: string): T | undefined {
    for (const bucket of this.buckets) {
      const p = bucket.find((x) => x.serverId === serverId);
      if (p) return p;
    }
    return undefined;
  }

  all(): T[] {
    const out: T[] = [];
    for (const bucket of this.buckets) out.push(...bucket);
    return out;
  }

  size(): number {
    let n = 0;
    for (const b of this.buckets) n += b.length;
    return n;
  }

  closestTo(targetHex: string, n: number): T[] {
    const t = hexToBigInt(targetHex);
    return this.all()
      .map((p) => ({ p, d: hexToBigInt(p.serverId) ^ t }))
      .sort((a, b) => (a.d < b.d ? -1 : a.d > b.d ? 1 : 0))
      .slice(0, n)
      .map((x) => x.p);
  }

  hungryBuckets(): number[] {
    const out: number[] = [];
    for (let i = 0; i < this.buckets.length; i++) {
      if (this.buckets[i].length < K) out.push(i);
    }
    return out;
  }

  snapshot(): { bucket: number; peers: T[] }[] {
    return this.buckets
      .map((peers, bucket) => ({ bucket, peers: [...peers] }))
      .filter((b) => b.peers.length > 0);
  }
}

export function generate44BitHexId(randomBytes6: Uint8Array): string {
  if (randomBytes6.length < 6) throw new Error("need 6 random bytes");
  const out = new Uint8Array(6);
  out.set(randomBytes6.subarray(0, 6));
  out[0] &= 0x0f;
  return Array.from(out).map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function normalizeIdHex(raw: string): string | null {
  const s = raw.trim().toLowerCase();
  if (!/^[0-9a-f]{1,12}$/.test(s)) return null;
  const n = BigInt("0x" + s);
  if (n > MAX_ID) return null;
  return n.toString(16).padStart(12, "0");
}

export { xorDistance };
