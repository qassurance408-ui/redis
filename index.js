const http = require('http');
const fs = require('fs');
const os = require('os');
const net = require('net');
const dns = require('dns').promises;
const { createClient } = require('redis');

const PORT = process.env.PORT || 3000;
const TIMEOUT = 4000;

// Candidate hosts: REDIS_URL host, plus anything in REDIS_HOSTS (comma-separated),
// falling back to the two names we want to compare.
function getTargets() {
  const targets = [];
  let password = process.env.REDIS_PASSWORD || undefined;
  let port = Number(process.env.REDIS_PORT || 6379);

  if (process.env.REDIS_URL) {
    try {
      const u = new URL(process.env.REDIS_URL);
      targets.push(u.hostname);
      if (u.port) port = Number(u.port);
      if (u.password) password = decodeURIComponent(u.password);
    } catch (e) {
      console.error('Invalid REDIS_URL:', e.message);
    }
  }
  if (process.env.REDIS_HOSTS) {
    targets.push(...process.env.REDIS_HOSTS.split(',').map(s => s.trim()).filter(Boolean));
  }
  if (targets.length === 0) {
    targets.push('internal-redis', 'internal-redis.app.aletcloud.com');
  }
  return { hosts: [...new Set(targets)], port, password };
}

function tcpCheck(host, port) {
  return new Promise(resolve => {
    const start = Date.now();
    const sock = net.connect({ host, port });
    const done = result => { sock.destroy(); resolve({ ...result, ms: Date.now() - start }); };
    sock.setTimeout(TIMEOUT, () => done({ ok: false, error: 'timeout' }));
    sock.once('connect', () => done({ ok: true }));
    sock.once('error', err => done({ ok: false, error: err.code || err.message }));
  });
}

async function redisCheck(host, port, password) {
  const client = createClient({
    socket: { host, port, connectTimeout: TIMEOUT, reconnectStrategy: false },
    password,
  });
  client.on('error', () => {}); // errors are surfaced via the awaited calls below
  const start = Date.now();
  try {
    await client.connect();
    const ping = await client.ping();
    const key = `redis-probe:${Date.now()}`;
    await client.set(key, 'ok', { EX: 60 });
    const value = await client.get(key);
    return { ok: true, ping, setGet: value === 'ok', ms: Date.now() - start };
  } catch (err) {
    return { ok: false, error: err.code || err.message, ms: Date.now() - start };
  } finally {
    try { await client.disconnect(); } catch {}
  }
}

async function checkHost(host, port, password) {
  const result = { host, port };
  try {
    const addrs = await dns.lookup(host, { all: true });
    result.dns = { ok: true, addresses: addrs.map(a => a.address) };
  } catch (err) {
    result.dns = { ok: false, error: err.code || err.message };
    return result;
  }
  result.tcp = await tcpCheck(host, port);
  if (result.tcp.ok) result.redis = await redisCheck(host, port, password);
  return result;
}

async function runChecks() {
  const { hosts, port, password } = getTargets();
  let resolvConf = null;
  try { resolvConf = fs.readFileSync('/etc/resolv.conf', 'utf8'); } catch {}
  const results = [];
  for (const host of hosts) results.push(await checkHost(host, port, password));
  return {
    time: new Date().toISOString(),
    podHostname: os.hostname(),
    passwordSet: Boolean(password),
    resolvConf, // the "search" line reveals the pod's namespace
    results,
  };
}

http.createServer(async (req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  const report = await runChecks();
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(report, null, 2));
}).listen(PORT, async () => {
  console.log(`redis-probe listening on ${PORT}`);
  const report = await runChecks();
  console.log(JSON.stringify(report, null, 2));
});
