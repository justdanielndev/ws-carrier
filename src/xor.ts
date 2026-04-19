export function hexToBigInt(hex: string): bigint {
  return BigInt("0x" + hex);
}

export function xorDistance(aHex: string, bHex: string): bigint {
  return hexToBigInt(aHex) ^ hexToBigInt(bHex);
}

export function closestBy<T>(
  targetHex: string,
  items: Iterable<T>,
  keyOf: (item: T) => string,
): T | undefined {
  let best: T | undefined;
  let bestDist: bigint | undefined;
  for (const it of items) {
    const d = xorDistance(targetHex, keyOf(it));
    if (bestDist === undefined || d < bestDist) {
      best = it;
      bestDist = d;
    }
  }
  return best;
}
