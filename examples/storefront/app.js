const byId = (id) => document.getElementById(id);
let currentState;
let refreshing = false;

function notice(message, error = false) {
  byId('notice').textContent = message;
  byId('notice').className = error ? 'error' : '';
}

async function request(path, data) {
  const response = await fetch(path, data === undefined ? {} : {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Storefront-Request': '1' },
    body: JSON.stringify(data),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : 'Request failed');
  return result;
}

function cell(row, value, state) {
  const td = document.createElement('td');
  if (state) {
    const badge = document.createElement('span');
    badge.className = `badge ${state}`;
    badge.textContent = value;
    td.append(badge);
  } else td.textContent = value;
  row.append(td);
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    const state = await request('/api/state');
    currentState = state;
    if (state.relay_url) byId('console').href = state.relay_url;
    for (const service of ['warehouse', 'accounting']) {
      const online = state.services[service];
      byId(`${service}-status`).textContent = online ? 'Online' : 'Offline';
      byId(`${service}-status`).className = `badge ${online ? 'online' : 'offline'}`;
      byId(`${service}-toggle`).textContent = online ? 'Take offline' : 'Bring online';
      byId(`${service}-toggle`).disabled = false;
    }
    byId('order-count').textContent = state.totals.orders;
    byId('empty').hidden = state.orders.length > 0;
    byId('orders').replaceChildren(...state.orders.map((order) => {
      const row = document.createElement('tr');
      cell(row, order.id);
      cell(row, order.customer);
      cell(row, new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(order.total_cents / 100));
      cell(row, order.pending_events ? `${order.pending_events} pending` : 'Both accepted', order.pending_events ? 'pending' : 'online');
      cell(row, order.warehouse ? 'Fulfilled' : 'Waiting', order.warehouse ? 'online' : 'pending');
      cell(row, order.accounting ? 'Recorded' : 'Waiting', order.accounting ? 'online' : 'pending');
      return row;
    }));
    byId('outbox-panel').hidden = state.outbox.length === 0;
    byId('outbox').replaceChildren(...state.outbox.map((entry) => {
      const li = document.createElement('li');
      li.textContent = `${entry.order_id} · ${entry.event_type} · ${entry.blocked ? 'Needs operator attention' : 'Retrying'}${entry.last_error ? `: ${entry.last_error}` : ''}`;
      return li;
    }));
    byId('refresh').textContent = 'Live · updates every 2 seconds';
  } catch (error) {
    byId('refresh').textContent = 'Disconnected · trying again';
    notice(error.message, true);
  } finally { refreshing = false; }
}

byId('quantity').addEventListener('input', () => {
  const amount = Number(byId('quantity').value) * 18;
  byId('place-order').textContent = `Place sample order · $${amount.toFixed(2)}`;
});

byId('checkout').addEventListener('submit', async (event) => {
  event.preventDefault();
  byId('place-order').disabled = true;
  try {
    const result = await request('/api/orders', {
      customer: byId('customer').value, quantity: Number(byId('quantity').value),
    });
    notice(`Order ${result.order_id} saved. Its events are in the outbox.`);
    await refresh();
  } catch (error) { notice(error.message, true); }
  finally { byId('place-order').disabled = false; }
});

for (const service of ['warehouse', 'accounting']) {
  byId(`${service}-toggle`).addEventListener('click', async () => {
    if (!currentState) return;
    byId(`${service}-toggle`).disabled = true;
    try {
      const online = !currentState.services[service];
      await request(`/api/services/${service}`, { online });
      notice(`${service === 'warehouse' ? 'Warehouse' : 'Accounting'} is ${online ? 'online. Queued deliveries will arrive on Relay’s next retry.' : 'offline. Its deliveries will retry while the other service continues.'}`);
      await refresh();
    } catch (error) { notice(error.message, true); }
    finally { byId(`${service}-toggle`).disabled = false; }
  });
}

refresh();
setInterval(refresh, 2000);
