# Revision history for streamly-websockets

## 0.1.0.0 -- 2026-11-21

* A WebSocket client over TCP: `withConnection`, `send`, `receive`,
  `messages`, `ping`, `pongCount`, `close`; `withConnectionOver` for a
  connection of the caller, such as a TLS one.
* Frames written and read (`Frame`), and the opening handshake
  (`Handshake`) over streamly-http.
