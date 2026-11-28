# Revision history for streamly-websockets

## 0.1.0.0 -- 2026-11-28

* First release on Hackage.
* A WebSocket client over TCP: `withConnection`, `send`, `receive`,
  `messages`, `ping`, `pongCount`, `close`, within a time to open
  (`openTimeout`); `withConnectionOver` for a connection of the caller,
  such as a TLS one.
* The side of a server (`Streamly.External.Network.WebSocket.Server`):
  `serve`, `listen`, `serveSocket`, `serveSocketWith` and `acceptOver`. A
  connection is the same type on both sides.
* Frames written and read (`Frame`), their headers with a flatparse
  parser, and the opening handshake (`Handshake`) over streamly-http.
