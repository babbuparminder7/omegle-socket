const { createServer } = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const server = createServer();
const wss = new WebSocketServer({ server });

// Track all connected users and their metadata
const users = new Map();
// Queue holding users waiting for a match
let waitingQueue = [];

// Helper to broadcast to all connected clients
function broadcast(channel, data) {
  const payload = JSON.stringify({ channel, data });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

// Helper to send messages safely to a single socket
function send(ws, channel, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ channel, data }));
  }
}

// When a new WebSocket connection is established
wss.on('connection', (ws, req) => {
  
  // 1. Detect Country (Mocked to India for now. In production, use req.socket.remoteAddress + GeoIP library)
  const detectedCountry = "IN";
  const detectedCountryName = "India";

  users.set(ws, {
    id: Date.now(),
    partner: null,
    country: detectedCountry,
    countryName: detectedCountryName,
    msgCount: 0
  });

  // 2. IMMEDIATELY send selfCountry on connection so the client knows its location
  send(ws, 'selfCountry', {
    country: detectedCountry,
    countryName: detectedCountryName,
    available: true
  });

  // Broadcast current online count to update UI
  broadcast('peopleOnline', users.size);

  ws.on('message', (rawMessage) => {
    try {
      const parsed = JSON.parse(rawMessage.toString());
      const { channel, data } = parsed;
      const user = users.get(ws);

      if (!user) return;

      switch (channel) {
        // --- Meta & Status ---
        case 'heartbeat':
          break;

        case 'peopleOnline':
          send(ws, 'peopleOnline', users.size);
          break;

        case 'userAFK':
          if (user.partner) {
            send(user.partner, 'peerAFK', {
              timestamp: data?.timestamp || Date.now(),
              reason: data?.reason || 'window_blur',
              gracePeriodMinutes: 8
            });
          }
          break;

        case 'userActive':
          if (user.partner) {
            send(user.partner, 'peerActive', {
              timestamp: data?.timestamp || Date.now(),
              reason: data?.reason || 'window_focus',
              afkDurationSeconds: 0
            });
          }
          break;

        // --- Matchmaking Engine ---
        case 'match':
          // Disconnect existing partner if user was already chatting
          if (user.partner) {
            send(user.partner, 'disconnect', '');
            const partnerData = users.get(user.partner);
            if (partnerData) partnerData.partner = null;
            user.partner = null;
          }

          // Remove self from queue to prevent matching with self
          waitingQueue = waitingQueue.filter(client => client !== ws);
          user.msgCount = 0; 

          if (waitingQueue.length > 0) {
            // Stranger found! Remove them from the front of the queue
            const stranger = waitingQueue.shift();
            const strangerData = users.get(stranger);

            // Link partners in memory
            user.partner = stranger;
            strangerData.partner = ws;

            // 3. EXACT SEQUENCE: Send 'connected' first
            send(ws, 'connected', []);
            send(stranger, 'connected', []);

            // 4. EXACT SEQUENCE: Send 'peerCountry' immediately after
            send(ws, 'peerCountry', {
              country: strangerData.country || "IN",
              countryName: strangerData.countryName || "India"
            });

            send(stranger, 'peerCountry', {
              country: user.country || "IN",
              countryName: user.countryName || "India"
            });

          } else {
            // No one waiting -> add self to waiting queue
            waitingQueue.push(ws);

            if (data?.params?.preferSameCountry) {
              send(ws, 'countryWait', "We're prioritizing people from your country right now.");
            }
          }
          break;

        // --- Chat & Typing Routing ---
        case 'typing':
          if (user.partner) {
            send(user.partner, 'typing', data); 
          }
          break;

        case 'message':
          if (user.partner) {
            user.msgCount++;
            const msgText = (data || "").toLowerCase();
            
            // Basic Anti-Bot Filter
            if (msgText.includes('telegram @') || msgText.includes('snapchat:')) {
               send(ws, 'disconnect', ''); 
               send(user.partner, 'disconnect', ''); 
               const partnerData = users.get(user.partner);
               if (partnerData) partnerData.partner = null;
               user.partner = null;
               break;
            }

            send(user.partner, 'message', data);
          }
          break;

        case 'disconnect':
          waitingQueue = waitingQueue.filter(client => client !== ws);
          if (user.partner) {
            send(user.partner, 'disconnect', '');
            const partnerData = users.get(user.partner);
            if (partnerData) partnerData.partner = null;
            user.partner = null;
          }
          break;
      }
    } catch (err) {
      console.error('Error processing message:', err.message);
    }
  });

  ws.on('close', () => {
    const user = users.get(ws);

    waitingQueue = waitingQueue.filter(client => client !== ws);

    if (user && user.partner) {
      send(user.partner, 'disconnect', '');
      const partnerData = users.get(user.partner);
      if (partnerData) partnerData.partner = null;
    }

    users.delete(ws);
    broadcast('peopleOnline', users.size);
  });
});

server.listen(PORT, () => {
  console.log(`WebSocket server listening on port ${PORT}`);
});
