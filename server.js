const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const UPSTREAM_URL = "wss://omegleweb.io:8443/";

const server = new WebSocket.Server({ port: PORT });

server.on("connection", (client) => {
    console.log("Client connected");

    const upstream = new WebSocket(UPSTREAM_URL);

    upstream.on("open", () => {
        console.log("Connected to upstream");
    });

    // Your client → upstream
    client.on("message", (data, isBinary) => {
        console.log("CLIENT → UPSTREAM:", data.toString());

        if (upstream.readyState === WebSocket.OPEN) {
            upstream.send(data, { binary: isBinary });
        }
    });

    // Upstream → your client
    upstream.on("message", (data, isBinary) => {
        console.log("UPSTREAM → CLIENT:", data.toString());

        if (client.readyState === WebSocket.OPEN) {
            client.send(data, { binary: isBinary });
        }
    });

    client.on("close", () => {
        console.log("Client disconnected");
        upstream.close();
    });

    upstream.on("close", () => {
        console.log("Upstream disconnected");

        if (client.readyState === WebSocket.OPEN) {
            client.close();
        }
    });

    client.on("error", (err) => {
        console.error("Client:", err.message);
    });

    upstream.on("error", (err) => {
        console.error("Upstream:", err.message);
    });
});

console.log(`Relay listening on ${PORT}`);
