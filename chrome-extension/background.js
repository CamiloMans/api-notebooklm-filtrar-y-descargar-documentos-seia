const ALARM_NAME = 'myma_cookie_sync';
const COOKIE_URL_QUERIES = [
  'https://notebooklm.google.com/',
  'https://accounts.google.com/',
  'https://myaccount.google.com/',
  'https://www.google.com/',
  'https://google.com/',
  'https://www.google.cl/',
];
const COOKIE_DOMAIN_QUERIES = [
  '.google.com',
  '.google.cl',
  '.googleusercontent.com',
  'google.com',
  'notebooklm.google.com',
  'accounts.google.com',
];
const DEFAULT_INTERVAL_MIN = 2;
const ENDPOINT_PATH = '/api/v1/adenda/notebook/credentials';
const VALIDATE_PATH = '/auth/validate-cookies';
const REFRESH_PATH = '/api/v1/adenda/auth/refresh';
const ACCESS_TOKEN_SAFETY_MARGIN_SEC = 60;
const AUTH_COOKIE_NAME_RE = /^(SID|SIDCC|__Secure-[13]PSID|__Secure-[13]PSIDTS|__Secure-[13]PSIDCC|SAPISID|APISID|HSID|SSID|__Secure-[13]PAPISID|OSID|__Secure-OSID)$/;
const COOKIE_NAME_QUERIES = [
  'SID',
  'SIDCC',
  'APISID',
  'HSID',
  'SAPISID',
  'SSID',
  '__Secure-1PSID',
  '__Secure-3PSID',
  '__Secure-1PAPISID',
  '__Secure-3PAPISID',
  'OSID',
  '__Secure-OSID',
];

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
    'lastCookieSummary',
    'needsConfigure',
    'lastRefreshError',
    'lastRefreshStatus',
    'preferredStoreId',
    'lastStoreValidationErrors',
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
    lastCookieSummary: data.lastCookieSummary || null,
    needsConfigure: !!data.needsConfigure,
    lastRefreshError: data.lastRefreshError || null,
    lastRefreshStatus: Number(data.lastRefreshStatus) || 0,
    preferredStoreId: (data.preferredStoreId || '').trim(),
    lastStoreValidationErrors: Array.isArray(data.lastStoreValidationErrors)
      ? data.lastStoreValidationErrors
      : [],
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
  const remember = (cookies) => {
    for (const c of cookies || []) {
      const partitionKey = c.partitionKey ? JSON.stringify(c.partitionKey) : '';
      const storeId = c.storeId || '';
      const key = `${storeId}@${c.name}@${c.domain}@${c.path || '/'}@${partitionKey}`;
      if (!seen.has(key)) seen.set(key, c);
    }
  };

  for (const url of COOKIE_URL_QUERIES) {
    let cookies;
    try {
      cookies = await chrome.cookies.getAll({ url });
    } catch (err) {
      console.warn('[myma-cookie-sync] cookies.getAll failed for url', url, err);
      continue;
    }
    remember(cookies);
  }

  for (const domain of COOKIE_DOMAIN_QUERIES) {
    let cookies;
    try {
      cookies = await chrome.cookies.getAll({ domain });
    } catch (err) {
      console.warn('[myma-cookie-sync] cookies.getAll failed for', domain, err);
      continue;
    }
    remember(cookies);
  }

  for (const name of COOKIE_NAME_QUERIES) {
    let cookies;
    try {
      cookies = await chrome.cookies.getAll({ name });
    } catch (err) {
      console.warn('[myma-cookie-sync] cookies.getAll failed for name', name, err);
      continue;
    }
    remember(cookies);
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
    storeId: c.storeId || '',
  }));
}

function summarizeCookies(cookies) {
  const names = Array.from(new Set(cookies.map((c) => c.name))).sort();
  const domains = Array.from(new Set(cookies.map((c) => c.domain))).sort();
  const authCookieNames = names.filter((name) => AUTH_COOKIE_NAME_RE.test(name));
  const stores = new Map();
  for (const cookie of cookies) {
    const storeId = cookie.storeId || 'default';
    if (!stores.has(storeId)) {
      stores.set(storeId, {
        store_id: storeId,
        count: 0,
        names: new Set(),
        domains: new Set(),
      });
    }
    const entry = stores.get(storeId);
    entry.count += 1;
    entry.names.add(cookie.name);
    entry.domains.add(cookie.domain);
  }
  const cookiesByStore = Array.from(stores.values()).map((entry) => {
    const storeNames = Array.from(entry.names).sort();
    return {
      store_id: entry.store_id,
      count: entry.count,
      auth_cookie_names: storeNames.filter((name) => AUTH_COOKIE_NAME_RE.test(name)),
      domains: Array.from(entry.domains).sort().slice(0, 12),
      has_sid: storeNames.includes('SID'),
      has_secure_1psid: storeNames.includes('__Secure-1PSID'),
      has_secure_3psid: storeNames.includes('__Secure-3PSID'),
      has_osid: storeNames.includes('OSID'),
      has_secure_osid: storeNames.includes('__Secure-OSID'),
    };
  });
  return {
    count: cookies.length,
    domain_count: domains.length,
    domains: domains.slice(0, 20),
    auth_cookie_names: authCookieNames,
    selected_cookie_names: names.slice(0, 80),
    store_ids: Array.from(new Set(cookies.map((c) => c.storeId || 'default'))).sort(),
    cookies_by_store: cookiesByStore,
    sid_cookie_domains: cookies.filter((c) => c.name === 'SID').map((c) => c.domain).sort(),
    has_sid: names.includes('SID'),
    has_secure_1psid: names.includes('__Secure-1PSID'),
    has_secure_3psid: names.includes('__Secure-3PSID'),
    has_osid: names.includes('OSID'),
    has_secure_osid: names.includes('__Secure-OSID'),
  };
}

function normalizeStoreId(value) {
  return (value || 'default').toString().trim() || 'default';
}

function authCookieCount(cookies) {
  return new Set(
    (cookies || [])
      .map((cookie) => cookie && cookie.name)
      .filter((name) => name && AUTH_COOKIE_NAME_RE.test(name))
  ).size;
}

function groupCookiesByStore(cookies) {
  const grouped = new Map();
  for (const cookie of cookies || []) {
    const storeId = normalizeStoreId(cookie.storeId);
    if (!grouped.has(storeId)) grouped.set(storeId, []);
    grouped.get(storeId).push(cookie);
  }
  return grouped;
}

function buildStoreCandidates(cookies, preferredStoreId) {
  const grouped = groupCookiesByStore(cookies);
  const preferred = normalizeStoreId(preferredStoreId);
  const candidates = [];
  const seen = new Set();

  if (preferredStoreId && grouped.has(preferred)) {
    candidates.push({ storeId: preferred, cookies: grouped.get(preferred) });
    seen.add(preferred);
  }

  const sidStores = Array.from(grouped.entries())
    .filter(([storeId, storeCookies]) => !seen.has(storeId) && storeCookies.some((cookie) => cookie.name === 'SID'))
    .sort((left, right) => {
      const rightAuthCount = authCookieCount(right[1]);
      const leftAuthCount = authCookieCount(left[1]);
      if (rightAuthCount !== leftAuthCount) return rightAuthCount - leftAuthCount;
      if (right[1].length !== left[1].length) return right[1].length - left[1].length;
      return left[0].localeCompare(right[0]);
    })
    .map(([storeId, storeCookies]) => ({ storeId, cookies: storeCookies }));

  return [...candidates, ...sidStores];
}

function compactError(prefix, text, maxLength = 360) {
  const body = (text || '').toString().replace(/\s+/g, ' ').trim();
  const compactBody = body.length > maxLength ? `${body.slice(0, maxLength)}...` : body;
  return prefix ? `${prefix}: ${compactBody}` : compactBody;
}

function buildRefreshFailureMessage(status, text) {
  const body = compactError('', text, 320);
  const base = `Refresh HTTP ${status}${body ? `: ${body}` : ''}`;
  if (status === 400 || status === 401) {
    return (
      'Refresh token Supabase invalido o expirado. Vuelve a Configurar y sincronizar ahora '
      + `desde la web para guardar una sesion nueva en la extension. ${base}`
    );
  }
  return base;
}

function isRefreshConfigurationError(err) {
  const message = String((err && err.message) || err || '');
  return message.includes('Refresh token Supabase invalido o expirado')
    || message.includes('Falta refresh_token Supabase');
}

async function readJsonOrText(response) {
  const text = await response.text();
  if (!text) return { text, parsed: null };
  try {
    return { text, parsed: JSON.parse(text) };
  } catch (_err) {
    return { text, parsed: null };
  }
}

async function refreshAccessToken(cfg) {
  if (!cfg.refreshToken) {
    const msg = 'Falta refresh_token Supabase. Configurar en el popup o desde la web.';
    await setStatus({
      needsConfigure: true,
      lastRefreshError: msg,
      lastRefreshStatus: 0,
    });
    throw new Error(msg);
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
    const msg = `Network error en refresh: ${err && err.message ? err.message : err}`;
    await setStatus({
      lastRefreshError: msg,
      lastRefreshStatus: 0,
    });
    throw new Error(msg);
  }
  const text = await resp.text();
  if (!resp.ok) {
    const msg = buildRefreshFailureMessage(resp.status, text);
    await setStatus({
      needsConfigure: resp.status === 400 || resp.status === 401,
      lastRefreshError: msg,
      lastRefreshStatus: resp.status,
    });
    throw new Error(msg);
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch (err) {
    const msg = `Refresh response no JSON: ${text.slice(0, 200)}`;
    await setStatus({
      lastRefreshError: msg,
      lastRefreshStatus: resp.status,
    });
    throw new Error(msg);
  }
  const accessToken = (data.access_token || '').trim();
  const newRefreshToken = (data.refresh_token || '').trim();
  if (!accessToken || !newRefreshToken) {
    const msg = 'Refresh response no incluye access_token/refresh_token.';
    await setStatus({
      lastRefreshError: msg,
      lastRefreshStatus: resp.status,
    });
    throw new Error(msg);
  }
  const expiresAt = Number(data.expires_at) || (
    data.expires_in ? Math.floor(Date.now() / 1000) + Number(data.expires_in) : 0
  );
  await setStatus({
    accessToken,
    refreshToken: newRefreshToken,
    accessTokenExpiresAt: expiresAt,
    needsConfigure: false,
    lastRefreshError: null,
    lastRefreshStatus: resp.status,
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
    await setStatus({ lastError: msg, needsConfigure: true });
    throw new Error(msg);
  }

  const cookies = await collectGoogleCookies();
  const cookieSummary = summarizeCookies(cookies);
  if (!cookies.length) {
    const msg = 'No se encontraron cookies de Google. Inicia sesion en accounts.google.com en este browser.';
    await setStatus({ lastError: msg, lastCookieSummary: cookieSummary });
    throw new Error(msg);
  }
  await setStatus({ lastCookieSummary: cookieSummary });

  const url = cfg.backendUrl.replace(/\/+$/, '') + ENDPOINT_PATH;
  const validateUrl = cfg.backendUrl.replace(/\/+$/, '') + VALIDATE_PATH;

  let accessToken;
  try {
    accessToken = await getValidAccessToken(cfg);
  } catch (err) {
    const msg = `Refresh JWT fallo: ${err && err.message ? err.message : err}`;
    await setStatus({
      lastError: msg,
      lastSync: new Date().toISOString(),
      needsConfigure: isRefreshConfigurationError(err),
    });
    throw new Error(msg);
  }

  async function requestWithAccess(makeRequest) {
    let response = await makeRequest(accessToken);
    if (response.status === 401) {
      accessToken = await refreshAccessToken({ ...cfg, accessToken: '', accessTokenExpiresAt: 0 });
      response = await makeRequest(accessToken);
    }
    return response;
  }

  async function validateStorageState(storageState) {
    let response;
    try {
      response = await requestWithAccess((token) => fetch(validateUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${cfg.bearerToken}`,
          'X-Myma-User-JWT': token,
        },
        body: JSON.stringify({ cookies_text: JSON.stringify(storageState) }),
      }));
    } catch (err) {
      if (isRefreshConfigurationError(err)) {
        return {
          ok: false,
          needsConfigure: true,
          error: String((err && err.message) || err),
        };
      }
      return {
        ok: false,
        error: `Network error validando store: ${err && err.message ? err.message : err}`,
      };
    }

    const { text, parsed } = await readJsonOrText(response);
    if (!response.ok) {
      if (response.status === 404) {
        return {
          ok: false,
          missingValidateEndpoint: true,
          error: compactError(
            'El backend no expone /auth/validate-cookies. Actualiza y reinicia el backend antes de sincronizar con la extension v0.2.9',
            text
          ),
        };
      }
      return {
        ok: false,
        error: compactError(`Validate HTTP ${response.status}`, text),
      };
    }
    if (!parsed || parsed.ok !== true || parsed.token_fetch_ok === false) {
      return {
        ok: false,
        error: compactError('', (parsed && parsed.message) || 'La API no valido este store.'),
      };
    }
    return { ok: true };
  }

  const candidates = buildStoreCandidates(cookies, cfg.preferredStoreId);
  if (!candidates.length) {
    const msg = 'No se encontraron stores de Chrome con cookie SID para NotebookLM.';
    const storeErrors = [{ storeId: 'none', error: msg }];
    await setStatus({
      lastError: msg,
      lastSync: new Date().toISOString(),
      lastStoreValidationErrors: storeErrors,
    });
    throw new Error(msg);
  }

  const storeValidationErrors = [];
  let selectedCandidate = null;
  for (const candidate of candidates) {
    const storageState = { cookies: candidate.cookies, origins: [] };
    const validation = await validateStorageState(storageState);
    if (validation.missingValidateEndpoint) {
      const storeErrors = [{
        storeId: candidate.storeId,
        error: validation.error,
      }];
      await setStatus({
        lastError: validation.error,
        lastSync: new Date().toISOString(),
        lastStoreValidationErrors: storeErrors,
      });
      throw new Error(validation.error);
    }
    if (validation.needsConfigure) {
      await setStatus({
        lastError: validation.error,
        lastSync: new Date().toISOString(),
        needsConfigure: true,
        lastStoreValidationErrors: [{
          storeId: candidate.storeId,
          error: validation.error,
        }],
      });
      throw new Error(validation.error);
    }
    if (validation.ok) {
      selectedCandidate = { ...candidate, storageState };
      break;
    }
    storeValidationErrors.push({
      storeId: candidate.storeId,
      error: validation.error || 'Store invalido para NotebookLM.',
    });
  }

  if (!selectedCandidate) {
    const firstError = storeValidationErrors[0]?.error || 'Ningun store valido para NotebookLM.';
    const msg = `Ningun store de Chrome valido para NotebookLM. ${firstError}`;
    await setStatus({
      lastError: msg,
      lastSync: new Date().toISOString(),
      lastStoreValidationErrors: storeValidationErrors,
    });
    throw new Error(msg);
  }

  let resp;
  try {
    resp = await requestWithAccess((token) => fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${cfg.bearerToken}`,
        'X-Myma-User-JWT': token,
      },
      body: JSON.stringify({ cookies_text: JSON.stringify(selectedCandidate.storageState) }),
    }));
  } catch (err) {
    if (isRefreshConfigurationError(err)) {
      const msg = String((err && err.message) || err);
      await setStatus({ lastError: msg, lastSync: new Date().toISOString(), needsConfigure: true });
      throw new Error(msg);
    }
    const msg = `Network error contactando backend: ${err && err.message ? err.message : err}`;
    await setStatus({ lastError: msg, lastSync: new Date().toISOString() });
    throw new Error(msg);
  }

  const { text, parsed: parsedResponse } = await readJsonOrText(resp);
  if (!resp.ok) {
    const msg = `HTTP ${resp.status}: ${text.slice(0, 400)}`;
    await setStatus({ lastError: msg, lastSync: new Date().toISOString(), lastResponse: text.slice(0, 800) });
    throw new Error(msg);
  }
  if (parsedResponse && parsedResponse.valid === false) {
    const status = parsedResponse.status || 'needs_validation';
    const detail = parsedResponse.last_error || 'El backend guardo cookies, pero no pudo validarlas contra NotebookLM.';
    const msg = `Credenciales NotebookLM no validas (${status}): ${String(detail).slice(0, 300)}`;
    await setStatus({ lastError: msg, lastSync: new Date().toISOString(), lastResponse: text.slice(0, 800) });
    throw new Error(msg);
  }

  await setStatus({
    lastError: null,
    lastSync: new Date().toISOString(),
    lastResponse: text.slice(0, 800),
    preferredStoreId: selectedCandidate.storeId,
    lastStoreValidationErrors: storeValidationErrors,
    needsConfigure: false,
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

const ALLOWED_EXTERNAL_ORIGINS = [
  /^https:\/\/aplicaciones-myma\.onrender\.com$/,
  /^https:\/\/app\.myma\.cl$/,
  /^https:\/\/[a-z0-9-]+\.myma\.cl$/,
  /^http:\/\/localhost:(3001|5173)$/,
  /^http:\/\/127\.0\.0\.1:(3001|5173)$/,
];

function isAllowedExternalOrigin(origin) {
  if (!origin) return false;
  return ALLOWED_EXTERNAL_ORIGINS.some((re) => re.test(origin));
}

async function applyExternalConfig(payload) {
  const config = payload && typeof payload === 'object' ? payload : {};
  const update = {};
  if (typeof config.backendUrl === 'string') update.backendUrl = config.backendUrl.trim();
  if (typeof config.bearerToken === 'string') update.bearerToken = config.bearerToken.trim();
  if (typeof config.intervalMin !== 'undefined') {
    const n = Math.max(1, Math.min(60, Number(config.intervalMin) || DEFAULT_INTERVAL_MIN));
    update.intervalMin = n;
  }
  if (typeof config.refreshToken === 'string' && config.refreshToken.trim()) {
    const previous = await chrome.storage.local.get(['refreshToken']);
    update.refreshToken = config.refreshToken.trim();
    if ((previous.refreshToken || '') !== update.refreshToken) {
      update.accessToken = '';
      update.accessTokenExpiresAt = 0;
    }
  }
  if (Object.keys(update).length === 0) {
    return { changed: false };
  }
  update.lastError = null;
  update.lastRefreshError = null;
  update.lastRefreshStatus = 0;
  update.needsConfigure = false;
  await chrome.storage.local.set(update);
  if (typeof update.intervalMin !== 'undefined') {
    await setupAlarm(update.intervalMin);
  }
  return { changed: true, fields: Object.keys(update) };
}

chrome.runtime.onMessageExternal.addListener((msg, sender, sendResponse) => {
  const origin = sender && sender.origin ? String(sender.origin) : '';
  if (!isAllowedExternalOrigin(origin)) {
    sendResponse({ ok: false, error: `Origen no autorizado: ${origin}` });
    return false;
  }

  if (msg && msg.type === 'ping') {
    sendResponse({ ok: true, version: chrome.runtime.getManifest().version });
    return false;
  }

  if (msg && msg.type === 'configure') {
    applyExternalConfig(msg.config || msg)
      .then((result) => sendResponse({ ok: true, ...result }))
      .catch((err) => sendResponse({ ok: false, error: String((err && err.message) || err) }));
    return true;
  }

  if (msg && msg.type === 'sync_now') {
    syncNow()
      .then((body) => sendResponse({ ok: true, body: String(body || '').slice(0, 1000) }))
      .catch((err) => sendResponse({ ok: false, error: String((err && err.message) || err) }));
    return true;
  }

  if (msg && msg.type === 'get_status') {
    loadConfig().then((cfg) => {
      const safe = {
        backendUrl: cfg.backendUrl,
        intervalMin: cfg.intervalMin,
        lastSync: cfg.lastSync,
        lastError: cfg.lastError,
        hasBearer: Boolean(cfg.bearerToken),
        hasRefreshToken: Boolean(cfg.refreshToken),
        accessTokenExpiresAt: cfg.accessTokenExpiresAt || 0,
        needsConfigure: !!cfg.needsConfigure,
        lastRefreshError: cfg.lastRefreshError || null,
        lastRefreshStatus: cfg.lastRefreshStatus || 0,
        lastCookieSummary: cfg.lastCookieSummary || null,
        preferredStoreId: cfg.preferredStoreId || '',
        lastStoreValidationErrors: cfg.lastStoreValidationErrors || [],
      };
      sendResponse({ ok: true, status: safe });
    });
    return true;
  }

  sendResponse({ ok: false, error: 'Tipo de mensaje desconocido.' });
  return false;
});
