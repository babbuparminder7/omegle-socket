const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const UPSTREAM_URL = "wss://omegleweb.io:8443/";

const server = new WebSocket.Server({ port: PORT });

console.log("Relay listening on port:", PORT);

server.on("connection", (client, req) => {
    console.log("================================");
    console.log("CLIENT CONNECTED");
    console.log("Client IP:", req.socket.remoteAddress);

    const upstream = new WebSocket(UPSTREAM_URL);

    upstream.on("open", () => {
        console.log("UPSTREAM CONNECTED");

        console.log("Client state:", client.readyState);
        console.log("Upstream state:", upstream.readyState);
    });

    upstream.on("message", (data, isBinary) => {
        console.log("========== UPSTREAM MESSAGE ==========");
        console.log("Binary:", isBinary);
        console.log("Data:", isBinary ? data : data.toString());

        if (client.readyState === WebSocket.OPEN) {
            console.log("Forwarding upstream → client");

            client.send(data, {
                binary: isBinary
            });
        } else {
            console.log(
                "CLIENT NOT OPEN:",
                client.readyState
            );
        }
    });

    client.on("message", (data, isBinary) => {
        console.log("========== CLIENT MESSAGE ==========");
        console.log("Binary:", isBinary);
        console.log("Data:", isBinary ? data : data.toString());

        if (upstream.readyState === WebSocket.OPEN) {
            console.log("Forwarding client → upstream");

            upstream.send(data, {
                binary: isBinary
            });
        } else {
            console.log(
                "UPSTREAM NOT OPEN:",
                upstream.readyState
            );
        }
    });

    upstream.on("error", err => {
        console.error("========== UPSTREAM ERROR ==========");
        console.error(err);
    });

    upstream.on("close", (code, reason) => {
        console.log("========== UPSTREAM CLOSED ==========");
        console.log("Code:", code);
        console.log("Reason:", reason.toString());

        if (client.readyState === WebSocket.OPEN) {
            client.close();
        }
    });

    client.on("error", err => {
        console.error("========== CLIENT ERROR ==========");
        console.error(err);
    });

    client.on("close", (code, reason) => {
        console.log("========== CLIENT CLOSED ==========");
        console.log("Code:", code);
        console.log("Reason:", reason.toString());

        if (
            upstream.readyState === WebSocket.OPEN ||
            upstream.readyState === WebSocket.CONNECTING
        ) {
            upstream.close();
        }
    });
});
