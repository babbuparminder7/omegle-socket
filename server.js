// server.js - Node.js WebSocket Broadcast Server

const { createServer } = require('http');
const { WebSocketServer, WebSocket } = require('ws');

// 1. Dynamic Port Binding (Required for Render)
const PORT = process.env.PORT || 3000;

// 2. Create basic HTTP Server
const server = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('WebSocket Server is Live');
});

// 3. Attach WebSocket Server to HTTP Instance
const wss = new WebSocketServer({ server });

console.log('Initializing WebSocket Server...');

// 4. Handle Client Connections
wss.on('connection', (ws, req) => {
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`[+] New client connected from: ${clientIp}`);

  // Handle incoming messages from clients
  ws.on('message', (rawMessage) => {
    try {
      // Parse incoming payload
      const messageStr = rawMessage.toString();
      const parsed = JSON.parse(messageStr);

      console.log(`[Message Received] Channel: "${parsed.channel}"`, parsed.data || '');

      // Broadcast payload to all connected clients
      wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify({
            channel: parsed.channel || 'chat',
            data: parsed.data || parsed
          }));
        }
      });

    } catch (err) {
      console.error('Failed to parse or broadcast message:', err.message);
    }
  });

  // Handle errors
  ws.on('error', (error) => {
    console.error('[-] Client connection error:', error);
  });

  // Handle disconnects
  ws.on('close', () => {
    console.log('[-] Client disconnected');
  });
});

// 5. Keep connections alive (30-second ping interval to prevent Render sleeping)
const heartbeat = setInterval(() => {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.ping();
    }
  });
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeat);
});

// 6. Start listening
server.listen(PORT, () => {
  console.log(`Server is running and listening on port ${PORT}`);
});
