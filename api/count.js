// Real worldwide incense counter for Khunk.
// Backed by the project's Upstash Redis store (env vars injected by Vercel).
// GET            -> { count: N }
// POST {"n": k, "batch": "<id>"} -> adds k grains and returns { count }.
//   - k is clamped to MAX_PER_REQUEST so one call can never dump thousands.
//   - A batch id is processed at most once: client retries after a lost
//     response can never double-count.
//   - Per-IP rate limit (grains per minute) blunts scripted inflation.

const MAX_PER_REQUEST = 50;
const MAX_PER_MINUTE_PER_IP = 1200;
const KEY = "khunk_grains_total";

function redisEnv() {
  return {
    url: process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL,
    token: process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN,
  };
}

async function redisCommand(cmd) {
  const { url, token } = redisEnv();
  if (!url || !token) throw new Error("counter store not configured");
  const r = await fetch(url, {
    method: "POST",
    headers: { Authorization: "Bearer " + token, "Content-Type": "application/json" },
    body: JSON.stringify(cmd),
  });
  if (!r.ok) throw new Error("redis error " + r.status);
  const j = await r.json();
  return j.result;
}

async function currentCount() {
  const v = await redisCommand(["GET", KEY]);
  return parseInt(v, 10) || 0;
}

module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Cache-Control", "no-store");
  if (req.method === "OPTIONS") return res.status(204).end();
  try {
    if (req.method === "GET") {
      return res.status(200).json({ count: await currentCount() });
    }
    if (req.method === "POST") {
      let body = "";
      for await (const chunk of req) body += chunk;
      let n = 1, batch = null;
      try {
        const j = JSON.parse(body || "{}");
        if (Number.isFinite(j.n)) n = Math.max(0, Math.min(MAX_PER_REQUEST, Math.floor(j.n)));
        if (typeof j.batch === "string" && j.batch.length <= 64) batch = j.batch;
      } catch (e) {}
      if (n <= 0) return res.status(200).json({ count: await currentCount() });

      const ip = String(req.headers["x-forwarded-for"] || "unknown").split(",")[0].trim();
      const rlKey = "khunk_rl:" + ip + ":" + Math.floor(Date.now() / 60000);
      const rlUsed = parseInt(await redisCommand(["INCRBY", rlKey, n]), 10) || 0;
      if (rlUsed === n) await redisCommand(["EXPIRE", rlKey, 70]);
      if (rlUsed > MAX_PER_MINUTE_PER_IP) {
        return res.status(429).json({ error: "rate limited", count: await currentCount() });
      }

      if (batch) {
        const added = parseInt(await redisCommand(["SADD", "khunk_batches", batch]), 10);
        await redisCommand(["EXPIRE", "khunk_batches", 172800]);
        if (added === 0) {
          return res.status(200).json({ count: await currentCount(), duplicate: true });
        }
      }

      const v = await redisCommand(["INCRBY", KEY, n]);
      return res.status(200).json({ count: parseInt(v, 10) || 0 });
    }
    return res.status(405).json({ error: "method not allowed" });
  } catch (e) {
    return res.status(500).json({ error: "counter unavailable" });
  }
};
