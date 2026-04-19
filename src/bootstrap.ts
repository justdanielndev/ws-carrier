const DEFAULT_BOOTSTRAP_URL =
  "https://raw.githubusercontent.com/znp-consortium/directory/main/servers.json";

export interface BootstrapOptions {
  bootstrapUrl?: string;
  bootstrapServers?: string[];
  selfAddr?: string | null;
  log: (msg: string) => void;
}

export async function loadBootstrapPeers(opts: BootstrapOptions): Promise<string[]> {
  if (opts.bootstrapServers && opts.bootstrapServers.length > 0) {
    return dedupeAndFilter(opts.bootstrapServers, opts.selfAddr);
  }
  const url = opts.bootstrapUrl ?? DEFAULT_BOOTSTRAP_URL;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    const data = (await res.json()) as { servers?: Array<{ url?: string }> };
    const urls = (data.servers ?? [])
      .map((s) => s.url)
      .filter((u): u is string => typeof u === "string");
    return dedupeAndFilter(urls, opts.selfAddr);
  } catch (err) {
    opts.log(`bootstrap fetch failed from ${url}: ${(err as Error).message}`);
    return [];
  }
}

function dedupeAndFilter(urls: string[], selfAddr?: string | null): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of urls) {
    const u = raw.trim();
    if (!u) continue;
    if (selfAddr && normalize(u) === normalize(selfAddr)) continue;
    if (seen.has(u)) continue;
    seen.add(u);
    out.push(u);
  }
  return out;
}

function normalize(u: string): string {
  return u.replace(/\/+$/, "").toLowerCase();
}
