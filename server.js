const { createServer } = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const server = createServer();
const wss = new WebSocketServer({ server });

// Track all connected users and their metadata
const users = new Map();
// Array to hold users waiting for a stranger
let waitingQueue = [];

// Helper to broadcast to all connected clients
function broadcast(channel, data) {
  const payload = JSON.stringify({ channel, data });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

// Helper to send messages safely
function send(ws, channel, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ channel, data }));
  }
}

wss.on('connection', (ws) => {
  // Initialize user state
  users.set(ws, { 
    id: Date.now(), 
    partner: null, 
    country: null,
    countryName: null,
    msgCount: 0 // Used for spam mitigation
  });

  // Notify everyone a new user joined
  broadcast('peopleOnline', users.size);

  ws.on('message', (rawMessage) => {
    try {
      const parsed = JSON.parse(rawMessage.toString());
      const { channel, data } = parsed;
      const user = users.get(ws);

      switch (channel) {
        
        // --- 1. Meta & Status Events ---
        case 'heartbeat':
          // The client pings periodically to keep the connection alive. No response needed.
          break;

        case 'peopleOnline':
          send(ws, 'peopleOnline', users.size);
          break;

        case 'selfCountry':
          if (data && data.country) {
            user.country = data.country;
            user.countryName = data.countryName;
          }
          break;

        case 'userAFK':
          // Forward AFK status to the partner if connected
          if (user.partner) {
            send(user.partner, 'peerAFK', { 
              timestamp: data.timestamp, 
              reason: data.reason, 
              gracePeriodMinutes: 8 
            });
          }
          break;

        case 'userActive':
          if (user.partner) {
            send(user.partner, 'peerActive', { 
              timestamp: data.timestamp, 
              reason: data.reason, 
              afkDurationSeconds: 0 
            });
          }
          break;


        // --- 2. Matchmaking Engine ---
        case 'match':
          // Disconnect existing partner if matched
          if (user.partner) {
            send(user.partner, 'disconnect', '');
            const partnerData = users.get(user.partner);
            if (partnerData) partnerData.partner = null;
            user.partner = null;
          }

          // Remove self from queue to prevent self-matching
          waitingQueue = waitingQueue.filter(client => client !== ws);
          user.msgCount = 0; // Reset spam counter for new match

          // Extract match parameters
          const preferSameCountry = data?.params?.preferSameCountry;
          const interests = data?.params?.interests || [];

          if (waitingQueue.length > 0) {
            let strangerIndex = 0;

            // Optional: Implement logic here to loop through waitingQueue 
            // and find someone with a matching country or interests.
            // For now, it grabs the first person in the queue.
            const stranger = waitingQueue.splice(strangerIndex, 1)[0];
            const strangerData = users.get(stranger);

            // Pair them up
            user.partner = stranger;
            strangerData.partner = ws;

            // Notify both clients they are connected
            send(ws, 'connected', []);
            send(stranger, 'connected', []);

            // Swap country info
            if (strangerData.country) {
              send(ws, 'peerCountry', { country: strangerData.country, countryName: strangerData.countryName });
            }
            if (user.country) {
              send(stranger, 'peerCountry', { country: user.country, countryName: user.countryName });
            }
          } else {
            // No one waiting. Add self to the queue.
            waitingQueue.push(ws);
            // Replicate the clone's UI message when queuing with country preference
            if (preferSameCountry) {
              send(ws, 'countryWait', "We're prioritizing people from your country right now.");
            }
          }
          break;


        // --- 3. 1-on-1 Chat Routing ---
        case 'typing':
          if (user.partner) {
            send(user.partner, 'typing', data); // data is true/false
          }
          break;

        case 'message':
          if (user.partner) {
            // Basic Anti-Bot/Spam Mitigation
            user.msgCount++;
            const msgText = (data || "").toLowerCase();
            
            // Drop instant telegram/snapchat bot links commonly found in these clones
            if (msgText.includes('telegram @') || msgText.includes('snapchat:')) {
               send(ws, 'disconnect', ''); // Boot the spammer
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
      console.error('Invalid JSON received or processing error:', err.message);
    }
  });

  ws.on('close', () => {
    const user = users.get(ws);
    
    // Clean up queue
    waitingQueue = waitingQueue.filter(client => client !== ws);
    
    // Clean up partner connection
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
  console.log(`WebSocket Matchmaking Server listening on port ${PORT}`);
});
