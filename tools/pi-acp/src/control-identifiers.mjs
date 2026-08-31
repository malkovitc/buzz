export function rpcResult(id, result) {
  return { jsonrpc: "2.0", id, result };
}

export function rpcError(id, code, message) {
  return { jsonrpc: "2.0", id: id ?? null, error: { code, message } };
}

function canonicalUuid(value) {
  if (typeof value !== "string") return null;
  let normalized = value.trim().toLowerCase();
  if (normalized.startsWith("urn:uuid:")) normalized = normalized.slice(9);
  if (normalized.startsWith("{") && normalized.endsWith("}")) {
    normalized = normalized.slice(1, -1);
  }
  normalized = normalized.replaceAll("-", "");
  if (!/^[0-9a-f]{32}$/.test(normalized)) return null;
  return [
    normalized.slice(0, 8),
    normalized.slice(8, 12),
    normalized.slice(12, 16),
    normalized.slice(16, 20),
    normalized.slice(20),
  ].join("-");
}

export function sameControlChannel(configured, actual) {
  const expected = canonicalUuid(configured);
  return expected !== null && expected === canonicalUuid(actual);
}

export function canonicalCloudRelayUrl(value) {
  const relay = new URL(value);
  if (
    !["wss:", "https:"].includes(relay.protocol) ||
    relay.username ||
    relay.password
  ) {
    throw new Error("cloud control relay URL is invalid");
  }
  if (relay.protocol === "https:") relay.protocol = "wss:";
  relay.hash = "";
  relay.search = "";
  relay.pathname = "/";
  return relay.toString().replace(/\/$/, "");
}
