const express = require('express');
const fs = require('fs');
const http = require('http');
const path = require('path');
const { randomUUID } = require('crypto');
const { WebSocket, WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const SOURCE_TOKEN = process.env.SOURCE_TOKEN || '';
const RING_CONTROLLER_URL = process.env.RING_CONTROLLER_URL ||
  'wss://ii-websocket-server-a9b7d506f512.herokuapp.com';
const SESSION_LOG_DIR = process.env.SESSION_LOG_DIR ||
  (process.env.RAILWAY_VOLUME_MOUNT_PATH
    ? path.join(process.env.RAILWAY_VOLUME_MOUNT_PATH, 'session-logs')
    : path.join(__dirname, 'data', 'session-logs'));

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
let currentHandsetState = null;
let ringRequest = null;
let relayRequest = null;
let restartRequest = null;
let speakerVolume = 75;
let aiVoiceVolume = 75;
let sourceHealthTimer = null;
let recentAssistantAudio = [];
const ASSISTANT_REPLAY_WINDOW_MS = 20000;

app.use(express.json());

fs.mkdirSync(SESSION_LOG_DIR, { recursive: true });

function safeLogId(value) {
  const id = typeof value === 'string' ? value.trim() : '';
  return /^[A-Za-z0-9._-]{1,120}$/.test(id) ? id : randomUUID();
}

function safeIsoDate(value, fallback) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? fallback : date.toISOString();
}

function normalizeSessionLog(raw) {
  if (!raw || typeof raw !== 'object') {
    throw new Error('Session log is missing');
  }

  const endedAt = safeIsoDate(raw.endedAt, new Date().toISOString());
  const startedAt = safeIsoDate(raw.startedAt, endedAt);
  const scenario = raw.scenario && typeof raw.scenario === 'object'
    ? {
        index: Number.isInteger(raw.scenario.index) ? raw.scenario.index : null,
        number: Number.isInteger(raw.scenario.number) ? raw.scenario.number : null,
        name: String(raw.scenario.name || 'Unknown scenario').slice(0, 200),
        voice: String(raw.scenario.voice || '').slice(0, 100)
      }
    : { index: null, number: null, name: 'Unknown scenario', voice: '' };

  let totalTextLength = 0;
  const messages = (Array.isArray(raw.messages) ? raw.messages : [])
    .slice(0, 1000)
    .map(message => {
      const text = String(message?.text || '').slice(0, 50000);
      totalTextLength += text.length;
      return {
        role: ['user', 'assistant', 'system'].includes(message?.role)
          ? message.role
          : 'unknown',
        text,
        timestamp: String(message?.timestamp || '').slice(0, 100),
        at: message?.at ? safeIsoDate(message.at, null) : null
      };
    });

  if (!messages.length) throw new Error('Session log contains no messages');
  if (totalTextLength > 2_000_000) throw new Error('Session log is too large');

  return {
    id: safeLogId(raw.id),
    startedAt,
    endedAt,
    disconnectReason: String(raw.disconnectReason || 'disconnect').slice(0, 100),
    scenario,
    messages,
    messageCount: messages.length,
    durationSeconds: Math.max(
      0,
      Math.round((new Date(endedAt).getTime() - new Date(startedAt).getTime()) / 1000)
    ),
    savedAt: new Date().toISOString()
  };
}

function sessionLogPath(id) {
  return path.join(SESSION_LOG_DIR, `${safeLogId(id)}.json`);
}

function saveSessionLog(raw) {
  const session = normalizeSessionLog(raw);
  const target = sessionLogPath(session.id);
  const temporary = `${target}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(session, null, 2), 'utf8');
  fs.renameSync(temporary, target);
  return session;
}

function readSessionLog(id) {
  if (!/^[A-Za-z0-9._-]{1,120}$/.test(id || '')) return null;
  const filename = sessionLogPath(id);
  if (!fs.existsSync(filename)) return null;
  return JSON.parse(fs.readFileSync(filename, 'utf8'));
}

function sessionLogSummary(session) {
  return {
    id: session.id,
    startedAt: session.startedAt,
    endedAt: session.endedAt,
    scenario: session.scenario,
    messageCount: session.messageCount ?? session.messages?.length ?? 0,
    durationSeconds: session.durationSeconds ?? 0
  };
}

function listSessionLogs() {
  return fs.readdirSync(SESSION_LOG_DIR)
    .filter(filename => filename.endsWith('.json'))
    .map(filename => {
      try {
        return sessionLogSummary(
          JSON.parse(fs.readFileSync(path.join(SESSION_LOG_DIR, filename), 'utf8'))
        );
      } catch (error) {
        console.warn(`Could not read session log ${filename}:`, error.message);
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => new Date(b.endedAt).getTime() - new Date(a.endedAt).getTime());
}

app.get('/api/session-logs', (req, res) => {
  try {
    res.json({ ok: true, logs: listSessionLogs() });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/api/session-logs/:id', (req, res) => {
  try {
    const session = readSessionLog(req.params.id);
    if (!session) return res.status(404).json({ ok: false, error: 'Session log not found' });
    return res.json({ ok: true, session });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/api/session-logs/:id/download', (req, res) => {
  try {
    const session = readSessionLog(req.params.id);
    if (!session) return res.status(404).send('Session log not found');
    const scenario = session.scenario || {};
    const lines = [
      '═══ THE PHONE - SESSION LOG ═══',
      `Started: ${new Date(session.startedAt).toLocaleString()}`,
      `Ended: ${new Date(session.endedAt).toLocaleString()}`,
      `Scenario: ${scenario.number || ''}. ${scenario.name || 'Unknown'}`.replace(': .', ':'),
      `Voice: ${scenario.voice || 'unknown'}`,
      `Messages: ${session.messages.length}`,
      ''
    ];
    for (const message of session.messages) {
      lines.push(`[${message.timestamp || ''}] ${String(message.role).toUpperCase()}:`);
      lines.push(message.text, '');
    }
    lines.push(`═══ Duration: ${session.durationSeconds || 0}s ═══`);
    const name = String(scenario.name || 'session').toLowerCase().replace(/[^a-z0-9]+/g, '_');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="the_phone_${scenario.number || 'x'}_${name}_${session.id}.txt"`
    );
    return res.type('text/plain').send(lines.join('\n'));
  } catch (error) {
    return res.status(500).send(error.message);
  }
});

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

function setPhoneAiVoiceVolume(level) {
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
      () => finish(new Error('Raspberry Pi AI volume controller did not respond')),
      10000
    );
    remote.on('open', () => {
      remote.send(JSON.stringify({ id: 'rpi_controller', message: `ai_volume:${level}` }));
    });
    remote.on('message', raw => {
      let message;
      try { message = JSON.parse(raw.toString()); } catch { return; }
      if (message.id !== 'rpi_status') return;
      if (message.message === `ai_volume_ok:${level}`) finish(null, { ok: true, level });
      else if (message.message === `ai_volume_error:${level}`) {
        finish(new Error(`Raspberry Pi could not set AI voice volume to ${level}%`));
      }
    });
    remote.on('error', error => finish(error));
    remote.on('close', () => {
      if (!settled) finish(new Error('Raspberry Pi AI volume controller disconnected'));
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

app.get('/ai/volume', (req, res) => {
  res.json({ ok: true, level: aiVoiceVolume });
});

app.post('/ai/volume', async (req, res) => {
  const level = Number(req.body?.level);
  if (!Number.isInteger(level) || level < 0 || level > 100) {
    return res.status(400).json({ ok: false, error: 'Volume must be an integer from 0 to 100' });
  }
  try {
    const result = await setPhoneAiVoiceVolume(level);
    aiVoiceVolume = result.level;
    broadcast({ type: 'ai_voice_volume', level: aiVoiceVolume });
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

    clearTimeout(sourceHealthTimer);
    sourceHealthTimer = setTimeout(() => {
      if (source === ws && ws.readyState === 1) {
        sendJson(ws, { type: 'health_control' });
      }
    }, 5000);

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

      if (msg.type === 'scenario_return_control_result') {
        broadcast(msg);
        return;
      }

      if (msg.type === 'physical_scenario_buttons_control_result') {
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

      if (msg.type === 'handset_state') {
        const event = {
          type: 'handset_state',
          lifted: !!msg.lifted,
          at: msg.at || new Date().toISOString()
        };
        currentHandsetState = event;
        if (!event.lifted) recentAssistantAudio = [];
        broadcast(event);
        return;
      }

      if (msg.type === 'session_log') {
        try {
          const session = saveSessionLog(msg.session);
          sendJson(ws, {
            type: 'session_log_saved',
            ok: true,
            id: session.id,
            messageCount: session.messageCount
          });
          broadcast({
            type: 'session_log_available',
            session: sessionLogSummary(session)
          });
        } catch (error) {
          sendJson(ws, {
            type: 'session_log_saved',
            ok: false,
            id: msg.session?.id || null,
            error: error.message
          });
        }
        return;
      }

      if (msg.type !== 'audio' || !msg.audio || !msg.channel) return;
      if (msg.channel === 'assistant' && currentHandsetState?.lifted === false) return;

      if (msg.channel === 'assistant') stats.assistantChunks++;
      if (msg.channel === 'user') stats.userChunks++;
      stats.lastAudioAt = new Date().toISOString();

      const audioEvent = {
        type: 'audio',
        channel: msg.channel,
        sampleRate: msg.sampleRate || 24000,
        audio: msg.audio
      };

      if (msg.channel === 'assistant') {
        const now = Date.now();
        recentAssistantAudio.push({ at: now, event: audioEvent });
        recentAssistantAudio = recentAssistantAudio.filter(
          entry => now - entry.at <= ASSISTANT_REPLAY_WINDOW_MS
        );
      }

      broadcast(audioEvent);
    });

    ws.on('close', () => {
      if (source === ws) {
        clearTimeout(sourceHealthTimer);
        sourceHealthTimer = null;
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
  if (currentHandsetState) {
    sendJson(ws, currentHandsetState);
  }
  sendJson(ws, { type: 'speaker_volume', level: speakerVolume });
  sendJson(ws, { type: 'ai_voice_volume', level: aiVoiceVolume });
  if (url.searchParams.get('replay') === '1') {
    const cutoff = Date.now() - ASSISTANT_REPLAY_WINDOW_MS;
    for (const entry of recentAssistantAudio) {
      if (entry.at >= cutoff) sendJson(ws, entry.event);
    }
  }

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

    if (msg.type === 'scenario_return_control') {
      if (typeof msg.enabled !== 'boolean') {
        sendJson(ws, {
          type: 'scenario_return_control_result',
          ok: false,
          message: 'Invalid return-to-default setting'
        });
        return;
      }
      if (!source || source.readyState !== 1) {
        sendJson(ws, {
          type: 'scenario_return_control_result',
          ok: false,
          message: 'Phone is not connected'
        });
        return;
      }
      sendJson(source, { type: 'scenario_return_control', enabled: msg.enabled });
      return;
    }

    if (msg.type === 'physical_scenario_buttons_control') {
      if (typeof msg.enabled !== 'boolean') {
        sendJson(ws, {
          type: 'physical_scenario_buttons_control_result',
          ok: false,
          message: 'Invalid physical-button setting'
        });
        return;
      }
      if (!source || source.readyState !== 1) {
        sendJson(ws, {
          type: 'physical_scenario_buttons_control_result',
          ok: false,
          message: 'Phone is not connected'
        });
        return;
      }
      sendJson(source, {
        type: 'physical_scenario_buttons_control',
        enabled: msg.enabled
      });
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
