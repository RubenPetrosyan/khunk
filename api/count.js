// Real worldwide incense counter for Khunk.
// Backed by the project's Upstash Redis store (env vars injected by Vercel).
// GET            -> { count: N }
// POST {"n": k}  -> adds k grains, returns { count: newTotal }

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

module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Cache-Control", "no-store");
  if (req.method === "OPTIONS") return res.status(204).end();
  try {
    if (req.method === "GET") {
      const v = await redisCommand(["GET", "khunk_grains_total"]);
      return res.status(200).json({ count: parseInt(v, 10) || 0 });
    }
    if (req.method === "POST") {
      let body = "";
      for await (const chunk of req) body += chunk;
      let n = 1;
      try {
        const j = JSON.parse(body || "{}");
        if (Number.isFinite(j.n)) n = Math.max(0, Math.min(10000, Math.floor(j.n)));
      } catch (e) {}
      const v = await redisCommand(["INCRBY", "khunk_grains_total", n]);
      return res.status(200).json({ count: parseInt(v, 10) || 0 });
    }
    return res.status(405).json({ error: "method not allowed" });
  } catch (e) {
    return res.status(500).json({ error: "counter unavailable" });
  }
};
