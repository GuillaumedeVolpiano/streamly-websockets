# streamly-websockets

WebSockets (RFC 6455) written with
[Streamly](https://streamly.composewell.com/), over plain TCP, for clients
and for servers. The opening handshake goes through
[streamly-http](https://github.com/GuillaumedeVolpiano/streamly-http);
frames are read with Streamly parsers from the stream of byte arrays of the
connection; the messages received are a stream.

A client:

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

A server that sends back what it receives:

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream
import Streamly.External.Network.WebSocket.Server

main :: IO ()
main = serve "127.0.0.1" 8080 defaultServerOptions $ \_ connection ->
  Stream.fold (Fold.drainMapM (send connection)) (messages connection)
```

## What it does

- Text and binary messages, sent in one frame each, received whole even
  when the other side fragments them. Text is checked to be UTF-8.
- Pings of the other side are answered, the pongs to `ping` counted
  (`pongCount`, in `STM`), and the closing handshake is done, all while
  messages are received.
- Sending is safe from several threads. Receiving is for one thread at a
  time.
- A limit on the size of a message (`maxMessageSize`) and on the time to
  open the connection (`openTimeout`).

## Modules

- `Streamly.External.Network.WebSocket`: the client.
- `Streamly.External.Network.WebSocket.Server`: the server. Each client is
  served in a thread of its own; every upgrade asked properly is accepted.
- `Streamly.External.Network.WebSocket.Frame`: frames, written and read.
- `Streamly.External.Network.WebSocket.Handshake`: the opening handshake.

## Limits of this version

- TLS is not built in: `withConnectionOver` and `acceptOver` take a
  connection opened by other means, as a `Transport` of streamly-http.
  [streamly-websockets-tls](https://github.com/GuillaumedeVolpiano/streamly-websockets-tls)
  does it for both sides: `wss://` URLs for clients, servers over TLS.
- No extension (no compression) and no subprotocol negotiation.
- Messages are sent in one frame; there is no streaming of a message.

## Building

The libraries this package is built on are not on Hackage yet:
`cabal.project` fetches them from GitHub, at their releases. To build against copies checked out next to
this repository instead, pass `--project-file=cabal.project.siblings`.
