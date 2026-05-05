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
  ]);
  $('backendUrl').value = data.backendUrl || '';
  $('bearerToken').value = data.bearerToken || '';
  $('refreshToken').value = data.refreshToken || '';
  $('intervalMin').value = data.intervalMin || 10;
  $('lastSync').textContent = data.lastSync || 'nunca';
  $('lastErr').textContent = data.lastError ? `Ultimo error: ${data.lastError}` : '';
  $('lastResponse').textContent = data.lastResponse || '';
}

async function save() {
  const intervalMin = Math.max(1, Math.min(60, Number($('intervalMin').value) || 10));
  const newRefresh = $('refreshToken').value.trim();
  const update = {
    backendUrl: $('backendUrl').value.trim(),
    bearerToken: $('bearerToken').value.trim(),
    refreshToken: newRefresh,
    intervalMin,
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
