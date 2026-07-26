# The Phone Live Audio Relay

Public WebSocket relay for streaming the installation audio to a browser.

## Run locally

```bash
npm install
npm start
```

Open:

```text
http://localhost:3000/live.html
```

## Deploy

Deploy this directory to a Node host that supports WebSockets, such as Railway.

After deployment, set the phone page live WebSocket URL to:

```text
wss://YOUR_DEPLOYMENT_HOST/ws?role=source
```

If `SOURCE_TOKEN` is set on the server, include it:

```text
wss://YOUR_DEPLOYMENT_HOST/ws?role=source&token=YOUR_TOKEN
```

## Session logs

The phone uploads each completed conversation after disconnect. Listener shows
the saved sessions in the **Session Logs** tab and exposes text downloads.

Logs are stored in `data/session-logs` by default. For durable production
storage, mount a Railway volume and set `SESSION_LOG_DIR` to a directory on
that volume. If `RAILWAY_VOLUME_MOUNT_PATH` is available, the server
automatically uses its `session-logs` subdirectory.
