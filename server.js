const { createServer } = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const server = createServer();
const wss = new WebSocketServer({ server });

const users = new Map();
let waitingQueue = [];

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

wss.on('connection', (ws) => {
  // 1. Detect Country (Mocked to IN/India. Replace with IP-based GeoIP later)
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

  // Tell the client its location immediately
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
          break;

        case 'peopleOnline':
          send(ws, 'peopleOnline', users.size);
          break;

        case 'userAFK':
          if (user.partner) {
            send(user.partner, 'peerAFK', { timestamp: Date.now(), reason: 'window_blur', gracePeriodMinutes: 8 });
          }
          break;

        case 'userActive':
          if (user.partner) {
            send(user.partner, 'peerActive', { timestamp: Date.now(), reason: 'window_focus', afkDurationSeconds: 0 });
          }
          break;

        case 'match':
          // Disconnect old partner if skipping
          if (user.partner) {
            send(user.partner, 'disconnect', '');
            const partnerData = users.get(user.partner);
            if (partnerData) partnerData.partner = null;
            user.partner = null;
          }

          waitingQueue = waitingQueue.filter(client => client !== ws);
          user.msgCount = 0; 
          
          // Save search preferences to the user's state
          user.interests = data?.params?.interests || [];
          user.preferSameCountry = data?.params?.preferSameCountry || false;

          let matchIndex = -1;
          let sharedInterests = [];

          // === THE MATCHMAKING ENGINE ===
          // PASS 1: Strict Match (Match Interests AND Match Country Preference)
          for (let i = 0; i < waitingQueue.length; i++) {
            const pData = users.get(waitingQueue[i]);
            
            // 1. Check Interests
            let interestMatch = false;
            let intersect = [];
            if (user.interests.length > 0 || pData.interests.length > 0) {
              intersect = getCommonInterests(user.interests, pData.interests);
              if (intersect.length > 0) interestMatch = true;
            } else {
              interestMatch = true; // Both users have no interests, they match.
            }
            if (!interestMatch) continue;

            // 2. Check Country Preference
            let countryMatch = true;
            if (user.preferSameCountry && user.country !== pData.country) countryMatch = false;
            if (pData.preferSameCountry && pData.country !== user.country) countryMatch = false;

            if (countryMatch) {
              matchIndex = i;
              sharedInterests = intersect;
              break;
            }
          }

          // PASS 2: Fallback Match (Ignore Country Preference, just match interests)
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
                break; // Stop at first valid interest match
              }
            }
          }

          // === RESULT HANDLING ===
          if (matchIndex !== -1) {
            // WE FOUND A STRANGER!
            const stranger = waitingQueue.splice(matchIndex, 1)[0];
            const strangerData = users.get(stranger);

            user.partner = stranger;
            strangerData.partner = ws;

            // Send "connected"
            send(ws, 'connected', []);
            send(stranger, 'connected', []);

            // Send "peerCountry" 
            send(ws, 'peerCountry', {
              country: strangerData.country || "IN",
              countryName: strangerData.countryName || "India"
            });
            send(stranger, 'peerCountry', {
              country: user.country || "IN",
              countryName: user.countryName || "India"
            });

            // (Crucial for your chat.js) Send the 'match' payload to unlock the UI and show common interests
            send(ws, 'match', {
              countryCode: strangerData.country || "IN",
              countryName: strangerData.countryName || "India",
              _pendingCommonInterests: sharedInterests 
            });
            send(stranger, 'match', {
              countryCode: user.country || "IN",
              countryName: user.countryName || "India",
              _pendingCommonInterests: sharedInterests
            });

          } else {
            // NO MATCH FOUND -> Add to Queue and send wait message
            waitingQueue.push(ws);

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
