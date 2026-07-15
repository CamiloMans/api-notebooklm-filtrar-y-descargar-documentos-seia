const $ = (id) => document.getElementById(id);

async function loadIntoForm() {
  const data = await chrome.storage.local.get([
    'backendUrl',
    'bearerToken',
    'refreshToken',
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
  $('backendUrl').value = data.backendUrl || '';
  $('bearerToken').value = data.bearerToken || '';
  $('refreshToken').value = data.refreshToken || '';
  $('intervalMin').value = data.intervalMin || 2;
  $('lastSync').textContent = data.lastSync || 'nunca';
  const refreshError = data.lastRefreshError
    ? `Ultimo refresh Supabase${data.lastRefreshStatus ? ` HTTP ${data.lastRefreshStatus}` : ''}: ${data.lastRefreshError}`
    : '';
  const configureWarning = data.needsConfigure
    ? 'Requiere configurar nuevamente desde la web: el refresh token Supabase guardado no sirve.'
    : '';
  $('lastErr').textContent = [configureWarning, data.lastError ? `Ultimo error: ${data.lastError}` : '', refreshError]
    .filter(Boolean)
    .join('\n');
  $('lastResponse').textContent = data.lastResponse || '';
  const summary = data.lastCookieSummary;
  const storeErrors = Array.isArray(data.lastStoreValidationErrors)
    ? data.lastStoreValidationErrors
    : [];
  $('cookieSummary').textContent = summary
    ? [
        `Cookies capturadas: ${summary.count || 0}`,
        `Stores: ${(summary.store_ids || []).join(', ') || '-'}`,
        `Store preferido: ${data.preferredStoreId || '-'}`,
        `Dominios: ${(summary.domains || []).join(', ') || '-'}`,
        `Auth cookies: ${(summary.auth_cookie_names || []).join(', ') || '-'}`,
        `SID: ${summary.has_sid ? 'si' : 'no'} | 1PSID: ${summary.has_secure_1psid ? 'si' : 'no'} | 3PSID: ${summary.has_secure_3psid ? 'si' : 'no'} | OSID: ${summary.has_osid ? 'si' : 'no'} | Secure-OSID: ${summary.has_secure_osid ? 'si' : 'no'}`,
        `SID dominios: ${(summary.sid_cookie_domains || []).join(', ') || '-'}`,
        ...(summary.cookies_by_store || []).map((store) => (
          `Store ${store.store_id}: ${store.count || 0} cookies | SID ${store.has_sid ? 'si' : 'no'} | auth ${(store.auth_cookie_names || []).join(', ') || '-'}`
        )),
        ...(storeErrors.length
          ? [
              'Errores validando stores:',
              ...storeErrors.map((item) => `- ${item.storeId || '-'}: ${item.error || 'sin detalle'}`),
            ]
          : []),
      ].join('\n')
    : '';
}

async function save() {
  const intervalMin = Math.max(1, Math.min(60, Number($('intervalMin').value) || 2));
  const newRefresh = $('refreshToken').value.trim();
  const update = {
    backendUrl: $('backendUrl').value.trim(),
    bearerToken: $('bearerToken').value.trim(),
    refreshToken: newRefresh,
    intervalMin,
    lastError: null,
    lastRefreshError: null,
    lastRefreshStatus: 0,
    needsConfigure: false,
  };
  const previous = await chrome.storage.local.get(['refreshToken']);
  if ((previous.refreshToken || '') !== newRefresh) {
    update.accessToken = '';
    update.accessTokenExpiresAt = 0;
  }
  await chrome.storage.local.set(update);
  await chrome.runtime.sendMessage({ type: 'set_interval', intervalMin });
  $('lastOk').textContent = 'Guardado.';
  setTimeout(() => ($('lastOk').textContent = ''), 1500);
}

async function syncNow() {
  $('lastErr').textContent = '';
  $('lastOk').textContent = 'Sincronizando...';
  try {
    const res = await chrome.runtime.sendMessage({ type: 'sync_now' });
    if (res && res.ok) {
      $('lastOk').textContent = 'Sync OK.';
    } else {
      $('lastOk').textContent = '';
      $('lastErr').textContent = (res && res.error) || 'Sync fallo.';
    }
  } catch (e) {
    $('lastOk').textContent = '';
    $('lastErr').textContent = String((e && e.message) || e);
  }
  await loadIntoForm();
}

async function grantHosts() {
  const url = ($('backendUrl').value || '').trim();
  if (!url) {
    $('lastErr').textContent = 'Ingresa backend URL antes de pedir permiso.';
    return;
  }
  let origin;
  try {
    origin = new URL(url).origin + '/*';
  } catch {
    $('lastErr').textContent = 'URL backend invalida.';
    return;
  }
  try {
    const granted = await chrome.permissions.request({ origins: [origin] });
    if (granted) {
      $('lastOk').textContent = `Permiso concedido para ${origin}`;
      $('lastErr').textContent = '';
    } else {
      $('lastErr').textContent = 'Permiso denegado por el usuario.';
    }
  } catch (err) {
    $('lastErr').textContent = `Error pidiendo permiso: ${err && err.message}`;
  }
}

function showExtensionId() {
  const id = chrome.runtime.id;
  $('extensionId').value = id;
}

async function copyExtId() {
  try {
    await navigator.clipboard.writeText($('extensionId').value);
    $('lastOk').textContent = 'Extension ID copiado.';
    setTimeout(() => ($('lastOk').textContent = ''), 1500);
  } catch (err) {
    $('lastErr').textContent = `Copy fallo: ${err && err.message}`;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  loadIntoForm();
  showExtensionId();
  $('save').addEventListener('click', save);
  $('syncNow').addEventListener('click', syncNow);
  $('grantHosts').addEventListener('click', grantHosts);
  $('copyExtId').addEventListener('click', copyExtId);
});
