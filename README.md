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
