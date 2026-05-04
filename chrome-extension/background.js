const ALARM_NAME = 'myma_cookie_sync';
const COOKIE_DOMAIN_QUERIES = ['.google.com', 'google.com', 'notebooklm.google.com', 'accounts.google.com'];
const DEFAULT_INTERVAL_MIN = 10;
const ENDPOINT_PATH = '/api/v1/adenda/notebook/credentials';

async function loadConfig() {
  const data = await chrome.storage.local.get([
    'backendUrl',
    'bearerToken',
    'userJwt',
    'intervalMin',
    'lastSync',
    'lastError',
    'lastResponse',
  ]);
  return {
    backendUrl: (data.backendUrl || '').trim(),
    bearerToken: (data.bearerToken || '').trim(),
    userJwt: (data.userJwt || '').trim(),
    intervalMin: Number(data.intervalMin) || DEFAULT_INTERVAL_MIN,
    lastSync: data.lastSync || null,
    lastError: data.lastError || null,
    lastResponse: data.lastResponse || null,
  };
}

async function setStatus(update) {
  await chrome.storage.local.set(update);
}

function normalizeSameSite(value) {
  const raw = (value || 'lax').toString().toLowerCase();
  if (raw === 'no_restriction' || raw === 'none') return 'None';
  if (raw === 'strict') return 'Strict';
  return 'Lax';
}

async function collectGoogleCookies() {
  const seen = new Map();
  for (const domain of COOKIE_DOMAIN_QUERIES) {
    let cookies;
    try {
      cookies = await chrome.cookies.getAll({ domain });
    } catch (err) {
      console.warn('[myma-cookie-sync] cookies.getAll failed for', domain, err);
      continue;
    }
    for (const c of cookies) {
      const key = `${c.name}@${c.domain}@${c.path || '/'}`;
      if (!seen.has(key)) seen.set(key, c);
    }
  }
  return Array.from(seen.values()).map((c) => ({
    name: c.name,
    value: c.value,
    domain: c.domain,
    path: c.path || '/',
    expires: typeof c.expirationDate === 'number' ? c.expirationDate : -1,
    httpOnly: !!c.httpOnly,
    secure: !!c.secure,
    sameSite: normalizeSameSite(c.sameSite),
  }));
}

async function syncNow() {
  const cfg = await loadConfig();
  if (!cfg.backendUrl || !cfg.bearerToken || !cfg.userJwt) {
    const msg = 'Falta configuracion: backend URL, bearer token o user JWT.';
    await setStatus({ lastError: msg });
    throw new Error(msg);
  }

  const cookies = await collectGoogleCookies();
  if (!cookies.length) {
    const msg = 'No se encontraron cookies de Google. Inicia sesion en accounts.google.com en este browser.';
    await setStatus({ lastError: msg });
    throw new Error(msg);
  }

  const storageState = { cookies, origins: [] };
  const url = cfg.backendUrl.replace(/\/+$/, '') + ENDPOINT_PATH;

  let resp;
  try {
    resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${cfg.bearerToken}`,
        'X-Myma-User-JWT': cfg.userJwt,
      },
      body: JSON.stringify({ cookies_text: JSON.stringify(storageState) }),
    });
  } catch (err) {
    const msg = `Network error contactando backend: ${err && err.message ? err.message : err}`;
    await setStatus({ lastError: msg, lastSync: new Date().toISOString() });
    throw new Error(msg);
  }

  const text = await resp.text();
  if (!resp.ok) {
    const msg = `HTTP ${resp.status}: ${text.slice(0, 400)}`;
    await setStatus({ lastError: msg, lastSync: new Date().toISOString(), lastResponse: text.slice(0, 800) });
    throw new Error(msg);
  }

  await setStatus({
    lastError: null,
    lastSync: new Date().toISOString(),
    lastResponse: text.slice(0, 800),
  });
  return text;
}

async function setupAlarm(intervalMin) {
  const minutes = Math.max(1, Math.min(60, Number(intervalMin) || DEFAULT_INTERVAL_MIN));
  await chrome.alarms.clear(ALARM_NAME);
  await chrome.alarms.create(ALARM_NAME, {
    delayInMinutes: 1,
    periodInMinutes: minutes,
  });
}

chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (alarm.name !== ALARM_NAME) return;
  try {
    await syncNow();
  } catch (err) {
    console.error('[myma-cookie-sync] sync alarm failed:', err);
  }
});

chrome.runtime.onInstalled.addListener(async () => {
  const cfg = await loadConfig();
  await setupAlarm(cfg.intervalMin);
});

chrome.runtime.onStartup.addListener(async () => {
  const cfg = await loadConfig();
  await setupAlarm(cfg.intervalMin);
});

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg && msg.type === 'sync_now') {
    syncNow()
      .then((body) => sendResponse({ ok: true, body }))
      .catch((err) => sendResponse({ ok: false, error: String((err && err.message) || err) }));
    return true;
  }
  if (msg && msg.type === 'set_interval') {
    setupAlarm(msg.intervalMin)
      .then(() => sendResponse({ ok: true }))
      .catch((err) => sendResponse({ ok: false, error: String(err) }));
    return true;
  }
  if (msg && msg.type === 'get_status') {
    loadConfig().then((cfg) => sendResponse({ ok: true, cfg }));
    return true;
  }
});
