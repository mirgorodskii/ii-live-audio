const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const SOURCE_TOKEN = process.env.SOURCE_TOKEN || '';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const listeners = new Set();
let source = null;
let stats = {
  assistantChunks: 0,
  userChunks: 0,
  connectedAt: null,
  lastAudioAt: null,
  lastWarningAt: null
};
let currentHealthState = null;

app.use(express.static(path.join(__dirname, 'public')));

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    sourceConnected: !!source,
    listeners: listeners.size,
    currentHealthState,
    stats
  });
});

function sendJson(ws, payload) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(payload) {
  const json = JSON.stringify(payload);
  for (const listener of listeners) {
    if (listener.readyState === 1) {
      listener.send(json);
    }
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const role = url.searchParams.get('role') || 'listener';
  const token = url.searchParams.get('token') || '';

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  if (role === 'source') {
    if (SOURCE_TOKEN && token !== SOURCE_TOKEN) {
      sendJson(ws, { type: 'error', message: 'Invalid source token' });
      ws.close(1008, 'Invalid token');
      return;
    }

    if (source && source.readyState === 1) {
      source.close(1012, 'Replaced by new source');
    }

    source = ws;
    stats.connectedAt = new Date().toISOString();
    broadcast({ type: 'source_status', connected: true });
    sendJson(ws, { type: 'ready', role: 'source', listeners: listeners.size });

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }

      if (msg.type === 'warning') {
        stats.lastWarningAt = new Date().toISOString();
        currentHealthState = {
          type: 'warning',
          level: msg.level || 'warning',
          message: msg.message || 'Phone health warning',
          at: msg.at || stats.lastWarningAt
        };
        broadcast(currentHealthState);
        return;
      }

      if (msg.type === 'health') {
        currentHealthState = {
          type: 'health',
          ok: !!msg.ok,
          message: msg.message || '',
          at: msg.at || new Date().toISOString()
        };
        broadcast(currentHealthState);
        return;
      }

      if (msg.type !== 'audio' || !msg.audio || !msg.channel) return;

      if (msg.channel === 'assistant') stats.assistantChunks++;
      if (msg.channel === 'user') stats.userChunks++;
      stats.lastAudioAt = new Date().toISOString();

      broadcast({
        type: 'audio',
        channel: msg.channel,
        sampleRate: msg.sampleRate || 24000,
        audio: msg.audio
      });
    });

    ws.on('close', () => {
      if (source === ws) {
        source = null;
        broadcast({ type: 'source_status', connected: false });
      }
    });

    return;
  }

  listeners.add(ws);
  sendJson(ws, {
    type: 'ready',
    role: 'listener',
    sourceConnected: !!source,
    listeners: listeners.size
  });
  if (currentHealthState) {
    sendJson(ws, currentHealthState);
  }

  broadcast({ type: 'listener_count', listeners: listeners.size });
  if (source) {
    sendJson(source, { type: 'listener_count', listeners: listeners.size });
  }

  ws.on('close', () => {
    listeners.delete(ws);
    broadcast({ type: 'listener_count', listeners: listeners.size });
    if (source) {
      sendJson(source, { type: 'listener_count', listeners: listeners.size });
    }
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`Live audio relay listening on ${PORT}`);
});
