const { createServer } = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const server = createServer();
const wss = new WebSocketServer({ server });

// --- State Management ---
// Track all users and their current metadata
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

// --- WebSocket Event Handling ---
wss.on('connection', (ws) => {
  // Initialize user state
  users.set(ws, { id: Date.now(), partner: null, country: null });

  ws.on('message', (rawMessage) => {
    try {
      const parsed = JSON.parse(rawMessage.toString());
      const { channel, data } = parsed;
      const user = users.get(ws);

      switch (channel) {
        
        // 1. Meta Events
        case 'heartbeat':
        case 'userActive':
        case 'userAFK':
          // Acknowledge or update activity timestamps internally
          break;

        case 'peopleOnline':
          // Send total online count back to the requester
          ws.send(JSON.stringify({ channel: 'peopleOnline', data: users.size }));
          break;

        case 'selfCountry':
          // Save the user's country preference
          user.country = data;
          break;

        // 2. Matchmaking Engine
        case 'match':
          // If already paired, disconnect the old partner first
          if (user.partner) {
            user.partner.send(JSON.stringify({ channel: 'disconnect', data: '' }));
            users.get(user.partner).partner = null;
            user.partner = null;
          }

          if (waitingQueue.length > 0) {
            // Match found! Pop the first user from the queue
            const stranger = waitingQueue.shift();
            
            // Pair them up in the state map
            user.partner = stranger;
            users.get(stranger).partner = ws;

            // Notify both clients they are connected
            const connectedPayload = JSON.stringify({ channel: 'connected', data: [] });
            ws.send(connectedPayload);
            stranger.send(connectedPayload);

            // Swap country info if available
            if (users.get(stranger).country) {
              ws.send(JSON.stringify({ channel: 'peerCountry', data: users.get(stranger).country }));
            }
            if (user.country) {
              stranger.send(JSON.stringify({ channel: 'peerCountry', data: user.country }));
            }

          } else {
            // No one waiting. Add self to the queue.
            if (!waitingQueue.includes(ws)) {
              waitingQueue.push(ws);
            }
          }
          break;

        // 3. 1-on-1 Chat Routing
        case 'message':
        case 'typing':
          // If the user has a partner, forward the message ONLY to them
          if (user.partner && user.partner.readyState === WebSocket.OPEN) {
            user.partner.send(JSON.stringify({ channel, data }));
          }
          break;

        case 'disconnect':
          // User manually skipped/disconnected
          if (user.partner) {
            user.partner.send(JSON.stringify({ channel: 'disconnect', data: '' }));
            users.get(user.partner).partner = null;
            user.partner = null;
          }
          break;
      }
    } catch (err) {
      console.error('Invalid JSON received:', err.message);
    }
  });

  ws.on('close', () => {
    const user = users.get(ws);
    
    // Remove from waiting queue if they were in it
    waitingQueue = waitingQueue.filter(client => client !== ws);
    
    // Notify partner if they were in an active chat
    if (user && user.partner && user.partner.readyState === WebSocket.OPEN) {
      user.partner.send(JSON.stringify({ channel: 'disconnect', data: '' }));
      users.get(user.partner).partner = null;
    }
    
    users.delete(ws);
    
    // Broadcast new user count
    broadcast('peopleOnline', users.size);
  });
});

server.listen(PORT, () => {
  console.log(`WebSocket Matchmaking Server listening on port ${PORT}`);
});
