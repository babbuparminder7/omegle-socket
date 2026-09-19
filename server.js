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

wss.on('connection', (ws) => {
  // 1. Initialize user with a fallback country 
  const initialCountry = {
    countryCode: "IN",
    countryName: "India"
  };

  users.set(ws, {
    id: Date.now(),
    partner: null,
    countryCode: initialCountry.countryCode,
    countryName: initialCountry.countryName,
    msgCount: 0
  });

  // (Optional) Tell the client its own country immediately (mirroring the network dump)
  send(ws, 'selfCountry', {
    country: initialCountry.countryCode,
    countryName: initialCountry.countryName,
    available: true
  });

  // Broadcast current online count to update UI immediately
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
          // Keep-alive ping from client, no action required
          break;

        case 'peopleOnline':
          send(ws, 'peopleOnline', users.size);
          break;

        case 'selfCountry':
          if (data && data.country) {
            user.countryCode = data.country;
            user.countryName = data.countryName || data.country;
          }
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
          // Disconnect existing partner if user was already chatting and hit "skip"
          if (user.partner) {
            send(user.partner, 'disconnect', '');
            const partnerData = users.get(user.partner);
            if (partnerData) partnerData.partner = null;
            user.partner = null;
          }

          // Remove self from queue to prevent matching with self
          waitingQueue = waitingQueue.filter(client => client !== ws);
          user.msgCount = 0; // Reset spam counter for new match

          if (waitingQueue.length > 0) {
            // Stranger found! Remove them from the front of the queue
            const stranger = waitingQueue.shift();
            const strangerData = users.get(stranger);

            // Link partners in memory
            user.partner = stranger;
            strangerData.partner = ws;

            // Send the exact 'match' event that chat.js expects
            // NOTE: It requires 'countryCode', not 'country'
            send(ws, 'match', {
              countryCode: strangerData.countryCode || "IN",
              countryName: strangerData.countryName || "India",
              _pendingCommonInterests: [] 
            });

            send(stranger, 'match', {
              countryCode: user.countryCode || "IN",
              countryName: user.countryName || "India",
              _pendingCommonInterests: []
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
            send(user.partner, 'typing', data); // data is a boolean
          }
          break;

        case 'message':
          if (user.partner) {
            user.msgCount++;
            const msgText = (data || "").toLowerCase();
            
            // Basic Anti-Bot Filter: Drop instant telegram/snapchat links common in these apps
            if (msgText.includes('telegram @') || msgText.includes('snapchat:')) {
               send(ws, 'disconnect', ''); // Boot the spammer
               send(user.partner, 'disconnect', ''); // Politely disconnect the innocent user
               const partnerData = users.get(user.partner);
               if (partnerData) partnerData.partner = null;
               user.partner = null;
               break;
            }

            send(user.partner, 'message', data);
          }
          break;

        case 'disconnect':
          // User manually pressed skip/disconnect
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

    // Remove from waiting queue if they drop connection
    waitingQueue = waitingQueue.filter(client => client !== ws);

    // Notify partner if they drop connection while actively chatting
    if (user && user.partner) {
      send(user.partner, 'disconnect', '');
      const partnerData = users.get(user.partner);
      if (partnerData) partnerData.partner = null;
    }

    users.delete(ws);
    // Broadcast updated online count to remaining users
    broadcast('peopleOnline', users.size);
  });
});

server.listen(PORT, () => {
  console.log(`WebSocket server listening on port ${PORT}`);
});
