# streamly-websockets

A WebSocket client (RFC 6455) written with
[Streamly](https://streamly.composewell.com/), over plain TCP. The opening
handshake goes through
[streamly-http](https://github.com/GuillaumedeVolpiano/streamly-http);
frames are read with Streamly parsers from the stream of byte arrays of the
connection; the messages received are a stream.

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream
import Streamly.External.Network.WebSocket

main :: IO ()
main = withConnection (config "127.0.0.1" 9330 "/api") $ \connection -> do
  send connection (TextMessage "hello")
  Stream.fold (Fold.drainMapM print) (Stream.take 3 (messages connection))
```

## What it does

- Text and binary messages, sent in one frame each, received whole even
  when the server fragments them. Text is checked to be UTF-8.
- Pings of the server are answered, the pongs to `ping` counted
  (`pongCount`, in `STM`), and the closing handshake is done, all while
  messages are received.
- Sending is safe from several threads. Receiving is for one thread at a
  time.
- A limit on the size of a message (`maxMessageSize`) and on the time to
  open the connection (`openTimeout`).

## Modules

- `Streamly.External.Network.WebSocket`: the client.
- `Streamly.External.Network.WebSocket.Frame`: frames, written and read.
- `Streamly.External.Network.WebSocket.Handshake`: the opening handshake.

## Limits of this version

- A client only: no server side.
- TLS is not built in. `withConnectionOver` takes a connection opened by
  other means, as a `Transport` of streamly-http: one of
  [streamly-network-tls](https://github.com/GuillaumedeVolpiano/streamly-network-tls)
  gives secure WebSockets (`wss://`).
- No extension (no compression) and no subprotocol negotiation.
- Messages are sent in one frame; there is no streaming of a message.

## Building

streamly-http is not on Hackage yet: `cabal.project` fetches it
from GitHub, at its release. To build against copies checked out next to
this repository instead, pass `--project-file=cabal.project.siblings`.
