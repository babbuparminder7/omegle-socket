const { createServer } = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const server = createServer();
const wss = new WebSocketServer({ server });

const users = new Map();
let waitingQueue = []; // Array of WebSocket objects

function broadcast(channel, data) {
  const payload = JSON.stringify({ channel, data });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

function send(ws, channel, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ channel, data }));
  }
}

// Helper to find common interests (case-insensitive)
function getCommonInterests(arr1, arr2) {
  if (!arr1 || !arr2) return [];
  const lowerArr2 = arr2.map(i => i.toLowerCase());
  return arr1.filter(i => lowerArr2.includes(i.toLowerCase()));
}

// --- QUEUE ANNOUNCER (Repeats wait messages) ---
// Runs every 30 seconds to tell waiting users we are still looking
setInterval(() => {
  waitingQueue.forEach(ws => {
    const user = users.get(ws);
    if (!user) return;
    
    if (user.interests && user.interests.length > 0) {
      send(ws, 'interestWait', "Finding someone who shares your interests may take a moment. If you get tired of waiting, you can");
    } else if (user.preferSameCountry) {
      send(ws, 'countryWait', "We're prioritizing people from your country right now.");
    }
  });
}, 30000); // 30 seconds (Adjust if you want it faster/slower)


wss.on('connection', (ws) => {
  // Default values (Replace with GeoIP lookup later if you want real locations)
  const detectedCountry = "IN";
  const detectedCountryName = "India";

  users.set(ws, {
    id: Date.now(),
    partner: null,
    country: detectedCountry,
    countryName: detectedCountryName,
    interests: [],
    preferSameCountry: false,
    msgCount: 0
  });

  // Tell client their own country on connect
  send(ws, 'selfCountry', {
    country: detectedCountry,
    countryName: detectedCountryName,
    available: true
  });

  broadcast('peopleOnline', users.size);

  ws.on('message', (rawMessage) => {
    try {
      const parsed = JSON.parse(rawMessage.toString());
      const { channel, data } = parsed;
      const user = users.get(ws);

      if (!user) return;

      switch (channel) {
        case 'heartbeat':
          // Keep-alive from client
          break;

        case 'peopleOnline':
          send(ws, 'peopleOnline', users.size);
          break;

        case 'userAFK':
          if (user.partner) send(user.partner, 'peerAFK', { timestamp: Date.now(), reason: 'window_blur', gracePeriodMinutes: 8 });
          break;

        case 'userActive':
          if (user.partner) send(user.partner, 'peerActive', { timestamp: Date.now(), reason: 'window_focus', afkDurationSeconds: 0 });
          break;

        case 'match':
          // 1. Disconnect existing partner if user is skipping
          if (user.partner) {
            send(user.partner, 'disconnect', '');
            const partnerData = users.get(user.partner);
            if (partnerData) partnerData.partner = null;
            user.partner = null;
          }

          // 2. Remove self from queue while processing
          waitingQueue = waitingQueue.filter(client => client !== ws);
          user.msgCount = 0; 
          
          // 3. Update search preferences from client payload
          user.interests = data?.params?.interests || [];
          user.preferSameCountry = data?.params?.preferSameCountry || false;

          let matchIndex = -1;
          let sharedInterests = [];

          // PASS 1: Strict Match (Interests AND Country)
          for (let i = 0; i < waitingQueue.length; i++) {
            const pData = users.get(waitingQueue[i]);
            let interestMatch = false;
            let intersect = [];
            
            if (user.interests.length > 0 || pData.interests.length > 0) {
              intersect = getCommonInterests(user.interests, pData.interests);
              if (intersect.length > 0) interestMatch = true;
            } else {
              interestMatch = true; // Both have 0 interests
            }
            if (!interestMatch) continue;

            let countryMatch = true;
            if (user.preferSameCountry && user.country !== pData.country) countryMatch = false;
            if (pData.preferSameCountry && pData.country !== user.country) countryMatch = false;

            if (countryMatch) {
              matchIndex = i;
              sharedInterests = intersect;
              break;
            }
          }

          // PASS 2: Fallback Match (Interests only, ignore country)
          if (matchIndex === -1) {
            for (let i = 0; i < waitingQueue.length; i++) {
              const pData = users.get(waitingQueue[i]);
              let interestMatch = false;
              let intersect = [];
              
              if (user.interests.length > 0 || pData.interests.length > 0) {
                intersect = getCommonInterests(user.interests, pData.interests);
                if (intersect.length > 0) interestMatch = true;
              } else {
                interestMatch = true; 
              }

              if (interestMatch) {
                matchIndex = i;
                sharedInterests = intersect;
                break; 
              }
            }
          }

          // === RESULT HANDLING ===
          if (matchIndex !== -1) {
            // MATCH FOUND!
            const stranger = waitingQueue.splice(matchIndex, 1)[0];
            const strangerData = users.get(stranger);

            user.partner = stranger;
            strangerData.partner = ws;

            // 1. Send 'connected' containing the array of shared interests
            // (e.g., {"channel":"connected","data":["love"]})
            send(ws, 'connected', sharedInterests);
            send(stranger, 'connected', sharedInterests);

            // 2. Send 'peerCountry' immediately after
            // (e.g., {"channel":"peerCountry","data":{"country":"NL","countryName":"Netherlands"}})
            send(ws, 'peerCountry', {
              country: strangerData.country || "IN",
              countryName: strangerData.countryName || "India"
            });
            send(stranger, 'peerCountry', {
              country: user.country || "IN",
              countryName: user.countryName || "India"
            });

          } else {
            // NO MATCH FOUND -> Add to Queue
            waitingQueue.push(ws);

            // Send the first wait message immediately
            if (user.interests.length > 0) {
              send(ws, 'interestWait', "Finding someone who shares your interests may take a moment. If you get tired of waiting, you can");
            } else if (user.preferSameCountry) {
              send(ws, 'countryWait', "We're prioritizing people from your country right now.");
            }
          }
          break;

        case 'typing':
          if (user.partner) send(user.partner, 'typing', data); 
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
