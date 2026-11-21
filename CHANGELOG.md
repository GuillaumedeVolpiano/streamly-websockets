# Revision history for streamly-websockets

## 0.3.0.0 -- 2026-11-21

* The header of a frame is a flatparse parser, run through
  streamly-flatparse: `frameHeader` changes its type.
* Built on streamly-http 0.3.

## 0.2.0.0 -- 2026-11-21

* `openTimeout` in `Config`: thirty seconds by default for the TCP
  connection and the answer to the upgrade, `OpenTimedOut` beyond.
* The side of a server: `Streamly.External.Network.WebSocket.Server`,
  with `serve`, `listen`, `serveSocket`, `serveSocketWith` and
  `acceptOver`. A connection is the same type on both sides.
* `readFrame` takes the side that reads: a server unmasks the frames of a
  client and refuses those that are not masked.
* The answer to the upgrade is at most 64 KiB.
* `listen` and the accept loop come from streamly-network-tcp.
* Built on streamly-http 0.2.

## 0.1.0.0 -- 2026-11-21

* A WebSocket client over TCP: `withConnection`, `send`, `receive`,
  `messages`, `ping`, `pongCount`, `close`; `withConnectionOver` for a
  connection of the caller, such as a TLS one.
* Frames written and read (`Frame`), and the opening handshake
  (`Handshake`) over streamly-http.
