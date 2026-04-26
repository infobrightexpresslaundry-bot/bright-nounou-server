const express = require('express');
const cors = require('cors');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json({ limit: '10mb' }));

// ── Data file paths ──
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

const FILES = {
  orders:    path.join(DATA_DIR, 'orders.json'),
  customers: path.join(DATA_DIR, 'customers.json'),
  trash:     path.join(DATA_DIR, 'trash.json'),
  loyalty:   path.join(DATA_DIR, 'loyalty.json'),
  counter:   path.join(DATA_DIR, 'counter.json'),
  logs:      path.join(DATA_DIR, 'logs.json'),
};

// ── Helpers ──
function readFile(file) {
  try {
    if (!fs.existsSync(file)) return null;
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch(e) { return null; }
}

function writeFile(file, data) {
  fs.writeFileSync(file, JSON.stringify(data), 'utf8');
}

// ── Broadcast to all connected PCs ──
function broadcast(type, data, senderWs) {
  const msg = JSON.stringify({ type, data, ts: Date.now() });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client !== senderWs) {
      client.send(msg);
    }
  });
}

// ══════════════════════════════════════
// REST API
// ══════════════════════════════════════

// Health check
app.get('/', (req, res) => res.json({ status: 'ok', name: 'Bright NouNou Server', clients: wss.clients.size }));

// ── Orders ──
app.get('/orders', (req, res) => {
  res.json(readFile(FILES.orders) || []);
});
app.post('/orders', (req, res) => {
  writeFile(FILES.orders, req.body);
  broadcast('orders_updated', req.body, null);
  res.json({ ok: true });
});

// ── Customers ──
app.get('/customers', (req, res) => {
  res.json(readFile(FILES.customers) || []);
});
app.post('/customers', (req, res) => {
  writeFile(FILES.customers, req.body);
  broadcast('customers_updated', req.body, null);
  res.json({ ok: true });
});

// ── Trash ──
app.get('/trash', (req, res) => {
  res.json(readFile(FILES.trash) || []);
});
app.post('/trash', (req, res) => {
  writeFile(FILES.trash, req.body);
  broadcast('trash_updated', req.body, null);
  res.json({ ok: true });
});

// ── Loyalty ──
app.get('/loyalty', (req, res) => {
  res.json(readFile(FILES.loyalty) || {});
});
app.post('/loyalty', (req, res) => {
  writeFile(FILES.loyalty, req.body);
  broadcast('loyalty_updated', req.body, null);
  res.json({ ok: true });
});

// ── Counter ──
app.get('/counter', (req, res) => {
  res.json(readFile(FILES.counter) || { value: 1050 });
});
app.post('/counter', (req, res) => {
  writeFile(FILES.counter, req.body);
  broadcast('counter_updated', req.body, null);
  res.json({ ok: true });
});

// ── Logs ──
app.get('/logs', (req, res) => {
  res.json(readFile(FILES.logs) || []);
});
app.post('/logs', (req, res) => {
  writeFile(FILES.logs, req.body);
  broadcast('logs_updated', req.body, null);
  res.json({ ok: true });
});

// ── Zoho token cache ──
let zohoTokenCache = { token: null, expiry: 0 };
app.get('/zoho-token', (req, res) => {
  if (zohoTokenCache.token && Date.now() < zohoTokenCache.expiry) {
    return res.json({ token: zohoTokenCache.token });
  }
  res.json({ token: null });
});
app.post('/zoho-token', (req, res) => {
  zohoTokenCache = { token: req.body.token, expiry: req.body.expiry };
  res.json({ ok: true });
});

// ── Zoho sync trigger — server polls Zoho and pushes to all PCs ──
app.post('/sync-event', (req, res) => {
  // One PC detected a change, broadcast to all others
  broadcast('sync_event', req.body, null);
  res.json({ ok: true });
});

// ══════════════════════════════════════
// WEBSOCKET — real-time push to all PCs
// ══════════════════════════════════════
wss.on('connection', (ws) => {
  console.log('PC connected. Total:', wss.clients.size);

  // Send current data immediately when PC connects
  ws.send(JSON.stringify({
    type: 'init',
    data: {
      orders:    readFile(FILES.orders)    || [],
      customers: readFile(FILES.customers) || [],
      trash:     readFile(FILES.trash)     || [],
      loyalty:   readFile(FILES.loyalty)   || {},
      counter:   readFile(FILES.counter)   || { value: 1050 },
      logs:      readFile(FILES.logs)      || [],
    }
  }));

  ws.on('message', (msg) => {
    try {
      const { type, data } = JSON.parse(msg);

      // Save to disk and broadcast to other PCs
      switch(type) {
        case 'save_orders':
          writeFile(FILES.orders, data);
          broadcast('orders_updated', data, ws);
          break;
        case 'save_customers':
          writeFile(FILES.customers, data);
          broadcast('customers_updated', data, ws);
          break;
        case 'save_trash':
          writeFile(FILES.trash, data);
          broadcast('trash_updated', data, ws);
          break;
        case 'save_loyalty':
          writeFile(FILES.loyalty, data);
          broadcast('loyalty_updated', data, ws);
          break;
        case 'save_counter':
          writeFile(FILES.counter, data);
          broadcast('counter_updated', data, ws);
          break;
        case 'save_logs':
          writeFile(FILES.logs, data);
          broadcast('logs_updated', data, ws);
          break;
        case 'ping':
          ws.send(JSON.stringify({ type: 'pong' }));
          break;
      }
    } catch(e) {
      console.log('WS message error:', e.message);
    }
  });

  ws.on('close', () => {
    console.log('PC disconnected. Total:', wss.clients.size);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Bright NouNou Server running on port ${PORT}`);
  console.log(`Data stored in: ${DATA_DIR}`);
});
