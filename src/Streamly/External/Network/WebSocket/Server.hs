-- |
-- Module      : Streamly.External.Network.WebSocket.Server
-- Description : A WebSocket server over TCP
-- License     : BSD-3-Clause
--
-- The side of a server: accepting the upgrade a client asks for, then
-- exchanging messages over the same 'Connection' as a client does.
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- > import Streamly.Data.Fold (drainMapM)
-- > import Streamly.Data.Stream (fold)
-- > import Streamly.External.Network.WebSocket.Server (defaultServerOptions,
-- >                                                    messages, send, serve)
-- >
-- > -- Send back what is received.
-- > main :: IO ()
-- > main = serve "127.0.0.1" 8080 defaultServerOptions $ \\_ connection ->
-- >   fold (drainMapM (send connection)) (messages connection)
--
-- Every upgrade asked properly is accepted, whatever its path: a handler
-- that does not want a client closes its connection. There is no TLS
-- ('acceptOver' takes a connection opened by other means) and no
-- extension.
module Streamly.External.Network.WebSocket.Server
  ( -- * Serving
    ServerOptions (..)
  , defaultServerOptions
  , Upgrade (..)
  , serve
  , listen
  , serveSocket
  , serveSocketWith
  , acceptOver

    -- * Connections
  , Connection
  , Message (..)
  , send
  , receive
  , messages
  , ping
  , pongCount
  , close

    -- * Errors
  , WebSocketError (..)
  ) where

import Control.Exception (SomeException, bracket, catch, throwIO, try)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Network.Socket (Socket, SocketOption (NoDelay), setSocketOption)
import Network.Socket qualified as N (close)
import Streamly.Data.StreamK (fromStream)
import Streamly.External.ByteString (toArray)
import Streamly.External.Network.HTTP (HttpError, Transport (..),
                                       socketTransport)
import Streamly.External.Network.HTTP.Wire (Header, RequestHead (..),
                                            readRequestHeadWithin)
import Streamly.External.Network.TCP (listen, serveConnections)
import Streamly.External.Network.WebSocket.Connection (Connection, Message (..),
                                                       close, established,
                                                       messages, ping,
                                                       pongCount, receive, send)
import Streamly.External.Network.WebSocket.Frame (Side (..),
                                                  WebSocketError (..))
import Streamly.External.Network.WebSocket.Handshake (checkUpgradeRequest,
                                                      refusal, upgradeResponse)
import System.Timeout (timeout)

-- | The bounds of the connections of a server.
data ServerOptions = ServerOptions
  { maxMessageSize :: Int
  -- ^ The largest message accepted, in bytes.
  , openTimeout :: Maybe Int
  -- ^ Microseconds allowed for a client to send its request. 'Nothing'
  -- waits without end.
  , closeTimeout :: Int
  -- ^ Microseconds to wait, once the handler is done, for the client to
  -- answer the closing of the connection.
  }
  deriving stock (Eq, Show)

-- | Messages of at most 64 MiB, thirty seconds for the request and one
-- second to close.
defaultServerOptions :: ServerOptions
defaultServerOptions = ServerOptions {maxMessageSize = 64 * 1024 * 1024, openTimeout = Just 30_000_000, closeTimeout = 1_000_000}

-- | What a client asked for.
data Upgrade = Upgrade
  { path :: ByteString
  -- ^ The path and the query, such as @\/chat?room=1@.
  , headers :: [Header]
  -- ^ The headers of the request, their names in lower case.
  }
  deriving stock (Eq, Show)

-- | Listen on an address and a port, and serve every client with a
-- handler, each in a thread of its own, without end. See 'serveSocket'.
serve :: String -> Int -> ServerOptions -> (Upgrade -> Connection -> IO ()) -> IO ()
serve host port options handler = bracket (listen host port) N.close (\listener -> serveSocket listener options handler)

-- | Serve every client that connects to a listening socket, each in a
-- thread of its own, without end. The connection of a client is closed
-- when its handler returns or throws; what a handler throws, and a client
-- whose request is refused, end that client only and are not reported.
serveSocket :: Socket -> ServerOptions -> (Upgrade -> Connection -> IO ()) -> IO ()
serveSocket listener options handler = serveSocketWith listener $ \peer ->
  acceptOver (socketTransport peer) options handler

-- | Like 'serveSocket', leaving to a function what is done with the TCP
-- connection of each client: a server over TLS does its handshake there,
-- then calls 'acceptOver' with the transport it got. The connection is
-- closed when the function returns or throws.
serveSocketWith :: Socket -> (Socket -> IO ()) -> IO ()
serveSocketWith listener client = serveConnections listener $ \peer -> do
  -- Frames are small and wait for answers: do not hold them back.
  setSocketOption peer NoDelay 1
  client peer

-- | Accept the upgrade asked on a connection that is already open, and
-- use the WebSocket. When the function returns, the connection is closed
-- in order. Opening and closing the transport are up to the caller.
--
-- Throws 'HandshakeFailed' when the client does not ask properly for a
-- WebSocket (it is answered 400), and 'OpenTimedOut' when its request does
-- not come in time.
acceptOver :: Transport -> ServerOptions -> (Upgrade -> Connection -> IO a) -> IO a
acceptOver transport options use = do
  (request, rest) <- opening $
    -- No request for an upgrade needs more than this.
    readRequestHeadWithin (Just 65536) (fromStream transport.receive)
      `catch` \(problem :: HttpError) -> refuse (HandshakeFailed (show problem))
  key <- either refuse pure (checkUpgradeRequest request)
  transport.send (toArray (upgradeResponse key))
  let RequestHead _ path headers = request
  established Server transport rest options.maxMessageSize options.closeTimeout (use Upgrade {path, headers})
  where
    refuse :: WebSocketError -> IO b
    refuse problem = do
      void (try @SomeException (transport.send (toArray refusal)))
      throwIO problem
    opening action = case options.openTimeout of
      Nothing -> action
      Just microseconds -> timeout microseconds action >>= maybe (throwIO OpenTimedOut) pure
