const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocket, WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const SOURCE_TOKEN = process.env.SOURCE_TOKEN || '';
const RING_CONTROLLER_URL = process.env.RING_CONTROLLER_URL ||
  'wss://ii-websocket-server-a9b7d506f512.herokuapp.com';

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
let currentScenarioState = null;
let ringRequest = null;
let relayRequest = null;
let restartRequest = null;
let speakerVolume = 75;

app.use(express.json());
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

function triggerPhoneRing() {
  if (ringRequest) return ringRequest;

  ringRequest = new Promise((resolve, reject) => {
    const remote = new WebSocket(RING_CONTROLLER_URL);
    let settled = false;
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { remote.close(); } catch {}
      if (error) reject(error);
      else resolve(result);
    };
    const timeout = setTimeout(
      () => finish(new Error('Raspberry Pi ring controller did not respond')),
      20000
    );

    remote.on('open', () => {
      remote.send(JSON.stringify({ id: 'rpi_controller', message: 'trigger' }));
    });
    remote.on('message', raw => {
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      if (message.id === 'rpi_status' && message.message === 'ok') {
        finish(null, { ok: true, message: 'Phone speaker rang' });
      }
    });
    remote.on('error', error => finish(error));
    remote.on('close', () => {
      if (!settled) finish(new Error('Raspberry Pi ring controller disconnected'));
    });
  }).finally(() => {
    ringRequest = null;
  });

  return ringRequest;
}

app.post('/ring', async (req, res) => {
  try {
    const result = await triggerPhoneRing();
    res.json(result);
  } catch (error) {
    res.status(503).json({ ok: false, error: error.message });
  }
});

function setPhoneRingVolume(level) {
  return new Promise((resolve, reject) => {
    const remote = new WebSocket(RING_CONTROLLER_URL);
    let settled = false;
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { remote.close(); } catch {}
      if (error) reject(error);
      else resolve(result);
    };
    const timeout = setTimeout(
      () => finish(new Error('Raspberry Pi volume controller did not respond')),
      10000
    );

    remote.on('open', () => {
      remote.send(JSON.stringify({ id: 'rpi_controller', message: `volume:${level}` }));
    });
    remote.on('message', raw => {
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      if (message.id === 'rpi_status' && message.message === `volume_ok:${level}`) {
        finish(null, { ok: true, level });
      } else if (message.id === 'rpi_status' && message.message === `volume_error:${level}`) {
        finish(new Error(`Raspberry Pi could not set ring volume to ${level}%`));
      }
    });
    remote.on('error', error => finish(error));
    remote.on('close', () => {
      if (!settled) finish(new Error('Raspberry Pi volume controller disconnected'));
    });
  });
}

app.post('/ring/volume', async (req, res) => {
  const level = Number(req.body?.level);
  if (!Number.isInteger(level) || level < 0 || level > 100) {
    return res.status(400).json({ ok: false, error: 'Volume must be an integer from 0 to 100' });
  }

  try {
    const result = await setPhoneRingVolume(level);
    speakerVolume = result.level;
    broadcast({ type: 'speaker_volume', level: speakerVolume });
    return res.json(result);
  } catch (error) {
    return res.status(503).json({ ok: false, error: error.message });
  }
});

app.get('/speaker/volume', (req, res) => {
  res.json({ ok: true, level: speakerVolume });
});

app.post('/speaker/volume', async (req, res) => {
  const level = Number(req.body?.level);
  if (!Number.isInteger(level) || level < 0 || level > 100) {
    return res.status(400).json({ ok: false, error: 'Volume must be an integer from 0 to 100' });
  }

  try {
    const result = await setPhoneRingVolume(level);
    speakerVolume = result.level;
    broadcast({ type: 'speaker_volume', level: speakerVolume });
    return res.json(result);
  } catch (error) {
    return res.status(503).json({ ok: false, error: error.message });
  }
});

function controlRelay(command) {
  if (relayRequest) return relayRequest;

  relayRequest = new Promise((resolve, reject) => {
    const remote = new WebSocket(RING_CONTROLLER_URL);
    let settled = false;
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { remote.close(); } catch {}
      if (error) reject(error);
      else resolve(result);
    };
    const timeout = setTimeout(
      () => finish(new Error('Raspberry Pi relay controller did not respond')),
      10000
    );

    remote.on('open', () => {
      remote.send(JSON.stringify({ id: 'rpi_controller', message: `relay:${command}` }));
    });
    remote.on('message', raw => {
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      const match = message.id === 'rpi_status' &&
        /^relay_state:(on|off)$/.exec(message.message || '');
      if (match) finish(null, { ok: true, state: match[1] });
      else if (message.id === 'rpi_status' && message.message === 'relay_error') {
        finish(new Error('Raspberry Pi could not switch the relay'));
      }
    });
    remote.on('error', error => finish(error));
    remote.on('close', () => {
      if (!settled) finish(new Error('Raspberry Pi relay controller disconnected'));
    });
  }).finally(() => {
    relayRequest = null;
  });

  return relayRequest;
}

app.get('/relay', async (req, res) => {
  try {
    return res.json(await controlRelay('get'));
  } catch (error) {
    return res.status(503).json({ ok: false, error: error.message });
  }
});

app.post('/relay/toggle', async (req, res) => {
  try {
    return res.json(await controlRelay('toggle'));
  } catch (error) {
    return res.status(503).json({ ok: false, error: error.message });
  }
});

function restartPhoneStack() {
  if (restartRequest) return restartRequest;

  restartRequest = new Promise((resolve, reject) => {
    const remote = new WebSocket(RING_CONTROLLER_URL);
    let settled = false;
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { remote.close(); } catch {}
      if (error) reject(error);
      else resolve(result);
    };
    const timeout = setTimeout(
      () => finish(new Error('Raspberry Pi restart controller did not respond')),
      10000
    );

    remote.on('open', () => {
      remote.send(JSON.stringify({ id: 'rpi_controller', message: 'restart' }));
    });
    remote.on('message', raw => {
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      if (message.id === 'rpi_status' && message.message === 'restart_accepted') {
        finish(null, { ok: true, message: 'Restart accepted; run.sh is starting' });
      }
    });
    remote.on('error', error => finish(error));
    remote.on('close', () => {
      if (!settled) finish(new Error('Raspberry Pi restart controller disconnected'));
    });
  }).finally(() => {
    restartRequest = null;
  });

  return restartRequest;
}

app.post('/restart', async (req, res) => {
  try {
    return res.json(await restartPhoneStack());
  } catch (error) {
    return res.status(503).json({ ok: false, error: error.message });
  }
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
        const event = {
          type: 'warning',
          kind: msg.kind || 'status',
          level: msg.level || 'warning',
          message: msg.message || 'Phone health warning',
          at: msg.at || stats.lastWarningAt
        };
        if (event.kind === 'test') currentHealthState = event;
        broadcast(event);
        return;
      }

      if (msg.type === 'scenario_state') {
        currentScenarioState = msg;
        broadcast(msg);
        return;
      }

      if (msg.type === 'scenario_control_result') {
        broadcast(msg);
        return;
      }

      if (msg.type === 'health') {
        const event = {
          type: 'health',
          kind: msg.kind || 'status',
          ok: !!msg.ok,
          message: msg.message || '',
          at: msg.at || new Date().toISOString()
        };
        if (event.kind === 'test') currentHealthState = event;
        broadcast(event);
        return;
      }

      if (msg.type === 'health_control_result') {
        broadcast(msg);
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
  if (currentScenarioState) {
    sendJson(ws, currentScenarioState);
  }
  sendJson(ws, { type: 'speaker_volume', level: speakerVolume });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.type === 'health_control') {
      if (!source || source.readyState !== 1) {
        sendJson(ws, { type: 'health_control_result', ok: false, message: 'Phone is not connected' });
        return;
      }
      sendJson(source, { type: 'health_control' });
      return;
    }
    if (msg.type !== 'scenario_control') return;

    const index = Number(msg.index);
    if (!['default', 'current'].includes(msg.mode) || !Number.isInteger(index) || index < 0 || index > 9) {
      sendJson(ws, { type: 'scenario_control_result', ok: false, message: 'Invalid scenario command' });
      return;
    }
    if (!source || source.readyState !== 1) {
      sendJson(ws, { type: 'scenario_control_result', ok: false, message: 'Phone is not connected' });
      return;
    }
    sendJson(source, { type: 'scenario_control', mode: msg.mode, index });
  });

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
