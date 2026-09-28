const http = require('http');
const fs = require('fs');
const os = require('os');
const net = require('net');
const dns = require('dns').promises;
const { createClient } = require('redis');

const PORT = process.env.PORT || 3000;
const TIMEOUT = 4000;

// Uses PROBE_* names on purpose: Kubernetes injects "service link" vars such as
// REDIS_PORT=tcp://10.x.x.x:6379 into pods, which collide with REDIS_* names.
function parsePort(value, fallback = 6379) {
  if (!value) return fallback;
  const m = String(value).match(/(\d+)\s*$/);
  const n = m ? Number(m[1]) : NaN;
  return n > 0 && n < 65536 ? n : fallback;
}

// Kubernetes sets <NAME>_SERVICE_HOST for every Service in the pod's namespace
// that existed when the pod started. This reveals the real service names.
function discoverServices() {
  const services = [];
  for (const [key, value] of Object.entries(process.env)) {
    const m = key.match(/^(.+)_SERVICE_HOST$/);
    if (!m || m[1] === 'KUBERNETES') continue;
    const prefix = m[1];
    services.push({
      envPrefix: prefix,
      likelyName: prefix.toLowerCase().replace(/_/g, '-'),
      clusterIP: value,
      port: process.env[`${prefix}_SERVICE_PORT`] || null,
    });
  }
  return services;
}

function getTargets(services) {
  const hosts = [];
  let password = process.env.PROBE_PASSWORD || undefined;
  let port = parsePort(process.env.PROBE_PORT);

  if (process.env.PROBE_URL) {
    try {
      const u = new URL(process.env.PROBE_URL);
      hosts.push(u.hostname);
      if (u.port) port = parsePort(u.port);
      if (u.password) password = decodeURIComponent(u.password);
    } catch (e) {
      console.error('Invalid PROBE_URL:', e.message);
    }
  }
  if (process.env.PROBE_HOSTS) {
    hosts.push(...process.env.PROBE_HOSTS.split(',').map(s => s.trim()).filter(Boolean));
  }
  // Always also try every discovered service plus the usual suspects.
  hosts.push(...services.filter(s => /redis/i.test(s.envPrefix)).map(s => s.likelyName));
  hosts.push('redis', 'internal-redis', 'internal-redis.app.aletcloud.com');
  return { hosts: [...new Set(hosts)], port, password };
}

function tcpCheck(host, port) {
  return new Promise(resolve => {
    const start = Date.now();
    let sock;
    const done = result => { if (sock) sock.destroy(); resolve({ ...result, ms: Date.now() - start }); };
    try {
      sock = net.connect({ host, port });
    } catch (err) {
      return done({ ok: false, error: err.code || err.message });
    }
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
  client.on('error', () => {});
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
  const services = discoverServices();
  const { hosts, port, password } = getTargets(services);
  let resolvConf = null;
  try { resolvConf = fs.readFileSync('/etc/resolv.conf', 'utf8'); } catch {}
  const results = [];
  for (const host of hosts) results.push(await checkHost(host, port, password));
  return {
    time: new Date().toISOString(),
    podHostname: os.hostname(),
    passwordSet: Boolean(password),
    rawRedisPortEnv: process.env.REDIS_PORT || null,
    servicesInNamespace: services,
    resolvConf, // the "search" line shows the pod's namespace
    results,
  };
}

http.createServer(async (req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  try {
    const report = await runChecks();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(report, null, 2));
  } catch (err) {
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end(String(err && err.stack || err));
  }
}).listen(PORT, async () => {
  console.log(`redis-probe listening on ${PORT}`);
  try {
    console.log(JSON.stringify(await runChecks(), null, 2));
  } catch (err) {
    console.error('Startup check failed:', err);
  }
});
