const ALARM_NAME = 'myma_cookie_sync';
const COOKIE_DOMAIN_QUERIES = ['.google.com', 'google.com', 'notebooklm.google.com', 'accounts.google.com'];
const DEFAULT_INTERVAL_MIN = 10;
const ENDPOINT_PATH = '/api/v1/adenda/notebook/credentials';
const REFRESH_PATH = '/api/v1/adenda/auth/refresh';
const ACCESS_TOKEN_SAFETY_MARGIN_SEC = 60;

async function loadConfig() {
  const data = await chrome.storage.local.get([
    'backendUrl',
    'bearerToken',
    'refreshToken',
    'accessToken',
    'accessTokenExpiresAt',
    'intervalMin',
    'lastSync',
    'lastError',
    'lastResponse',
  ]);
  return {
    backendUrl: (data.backendUrl || '').trim(),
    bearerToken: (data.bearerToken || '').trim(),
    refreshToken: (data.refreshToken || '').trim(),
    accessToken: (data.accessToken || '').trim(),
    accessTokenExpiresAt: Number(data.accessTokenExpiresAt) || 0,
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

async function refreshAccessToken(cfg) {
  if (!cfg.refreshToken) {
    throw new Error('Falta refresh_token Supabase. Configurar en el popup.');
  }
  const url = cfg.backendUrl.replace(/\/+$/, '') + REFRESH_PATH;
  let resp;
  try {
    resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${cfg.bearerToken}`,
      },
      body: JSON.stringify({ refresh_token: cfg.refreshToken }),
    });
  } catch (err) {
    throw new Error(`Network error en refresh: ${err && err.message ? err.message : err}`);
  }
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Refresh HTTP ${resp.status}: ${text.slice(0, 400)}`);
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch (err) {
    throw new Error(`Refresh response no JSON: ${text.slice(0, 200)}`);
  }
  const accessToken = (data.access_token || '').trim();
  const newRefreshToken = (data.refresh_token || '').trim();
  if (!accessToken || !newRefreshToken) {
    throw new Error('Refresh response no incluye access_token/refresh_token.');
  }
  const expiresAt = Number(data.expires_at) || (
    data.expires_in ? Math.floor(Date.now() / 1000) + Number(data.expires_in) : 0
  );
  await setStatus({
    accessToken,
    refreshToken: newRefreshToken,
    accessTokenExpiresAt: expiresAt,
  });
  return accessToken;
}

async function getValidAccessToken(cfg) {
  const nowSec = Math.floor(Date.now() / 1000);
  if (
    cfg.accessToken
    && cfg.accessTokenExpiresAt
    && cfg.accessTokenExpiresAt - ACCESS_TOKEN_SAFETY_MARGIN_SEC > nowSec
  ) {
    return cfg.accessToken;
  }
  return refreshAccessToken(cfg);
}

async function syncNow() {
  let cfg = await loadConfig();
  if (!cfg.backendUrl || !cfg.bearerToken || !cfg.refreshToken) {
    const msg = 'Falta configuracion: backend URL, bearer token o refresh_token.';
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

  async function postWithAccess(accessToken) {
    return fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${cfg.bearerToken}`,
        'X-Myma-User-JWT': accessToken,
      },
      body: JSON.stringify({ cookies_text: JSON.stringify(storageState) }),
    });
  }

  let accessToken;
  try {
    accessToken = await getValidAccessToken(cfg);
  } catch (err) {
    const msg = `Refresh JWT fallo: ${err && err.message ? err.message : err}`;
    await setStatus({ lastError: msg, lastSync: new Date().toISOString() });
    throw new Error(msg);
  }

  let resp;
  try {
    resp = await postWithAccess(accessToken);
  } catch (err) {
    const msg = `Network error contactando backend: ${err && err.message ? err.message : err}`;
    await setStatus({ lastError: msg, lastSync: new Date().toISOString() });
    throw new Error(msg);
  }

  if (resp.status === 401) {
    try {
      accessToken = await refreshAccessToken({ ...cfg, accessToken: '', accessTokenExpiresAt: 0 });
      resp = await postWithAccess(accessToken);
    } catch (err) {
      const msg = `401 + refresh fallo: ${err && err.message ? err.message : err}`;
      await setStatus({ lastError: msg, lastSync: new Date().toISOString() });
      throw new Error(msg);
    }
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
