(() => {
  'use strict';

  const authForm = document.querySelector('#operator-auth');
  const tokenInput = document.querySelector('#operator-token');
  const status = document.querySelector('#surface-status');
  const refresh = document.querySelector('#refresh-blocks');
  const tableWrap = document.querySelector('.table-wrap');
  const headings = document.querySelector('#column-headings');
  const rows = document.querySelector('#block-rows');
  const empty = document.querySelector('#empty-state');
  const rowTemplate = document.querySelector('#block-row');

  let contract;
  let operatorToken = '';

  function setStatus(message, kind = '') {
    status.textContent = message;
    status.dataset.kind = kind;
  }

  function request(path, options = {}) {
    return fetch(path, {
      ...options,
      headers: {
        Authorization: `Bearer ${operatorToken}`,
        ...(options.body ? { 'Content-Type': 'application/json' } : {})
      }
    });
  }

  async function requireSuccess(response) {
    if (response.ok) return response;
    let detail = `${response.status} ${response.statusText}`;
    try {
      const error = await response.json();
      detail = error.message || error.error || detail;
    } catch (_) {
      // The HTTP status remains authoritative when no JSON error is present.
    }
    throw new Error(detail);
  }

  function fieldContent(field, value) {
    if (field === 'message') {
      const pre = document.createElement('pre');
      pre.textContent = JSON.stringify(value, null, 2);
      return pre;
    }
    if (field === 'blocked_at_ms') {
      const time = document.createElement('time');
      time.dateTime = new Date(value).toISOString();
      time.textContent = `${value} (${new Date(value).toLocaleString()})`;
      return time;
    }
    const code = document.createElement('code');
    code.textContent = value == null ? '' : String(value);
    return code;
  }

  function actionCell(block) {
    const cell = document.createElement('td');
    cell.dataset.label = 'actions';
    const controls = document.createElement('div');
    controls.className = 'actions';
    for (const action of contract.actions) {
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = action.semantic.name;
      button.dataset.action = action.semantic.name;
      button.dataset.outcome = action.semantic.outcome;
      button.setAttribute('aria-label', `${action.semantic.name} block ${block[contract.selector_field]}`);
      button.addEventListener('click', () => resolve(block, action));
      controls.append(button);
    }
    cell.append(controls);
    return cell;
  }

  function render(blocks) {
    rows.replaceChildren();
    empty.hidden = blocks.length !== 0;
    for (const block of blocks) {
      const row = rowTemplate.content.firstElementChild.cloneNode(true);
      for (const field of contract.table_columns) {
        const cell = document.createElement('td');
        cell.dataset.label = field;
        cell.append(fieldContent(field, block[field]));
        row.append(cell);
      }
      row.append(actionCell(block));
      rows.append(row);
    }
    tableWrap.hidden = false;
  }

  async function loadBlocks() {
    if (!contract || !operatorToken) return;
    setStatus('Loading unresolved deliveries…');
    refresh.disabled = true;
    try {
      const response = await requireSuccess(await request(contract.binding.list_path, {
        method: contract.binding.list_method
      }));
      const blocks = await response.json();
      render(blocks);
      setStatus(`${blocks.length} unresolved ${blocks.length === 1 ? 'delivery' : 'deliveries'}.`, 'ok');
    } catch (error) {
      tableWrap.hidden = true;
      setStatus(`Unable to load blocks: ${error.message}`, 'error');
    } finally {
      refresh.disabled = false;
    }
  }

  async function resolve(block, action) {
    const selector = block[contract.selector_field];
    if (action.semantic.outcome === 'dead' && !window.confirm(`Abandon block ${selector}? It will not be retried.`)) {
      return;
    }
    const marker = `{${contract.binding.selector_parameter}}`;
    const path = contract.binding.resolve_path_template.replace(marker, encodeURIComponent(selector));
    setStatus(`${action.semantic.name} block ${selector}…`);
    try {
      await requireSuccess(await request(path, {
        method: contract.binding.resolve_method,
        body: JSON.stringify({
          resolution: action.semantic.name,
          retry_after_ms: 0,
          note: 'Resolved from the Fleetd operator surface'
        })
      }));
      await loadBlocks();
    } catch (error) {
      setStatus(`Unable to resolve block ${selector}: ${error.message}`, 'error');
    }
  }

  async function initialize() {
    try {
      contract = await requireSuccess(await fetch('/operator/contract.json')).then(response => response.json());
      headings.replaceChildren();
      for (const field of [...contract.table_columns, 'actions']) {
        const heading = document.createElement('th');
        heading.scope = 'col';
        heading.textContent = field;
        headings.append(heading);
      }
      setStatus('Enter the local operator token to load unresolved blocks.');
    } catch (error) {
      authForm.hidden = true;
      setStatus(`Unable to load the surface contract: ${error.message}`, 'error');
    }
  }

  authForm.addEventListener('submit', event => {
    event.preventDefault();
    operatorToken = tokenInput.value.trim();
    tokenInput.value = '';
    if (operatorToken) loadBlocks();
  });
  refresh.addEventListener('click', loadBlocks);
  initialize();
})();
