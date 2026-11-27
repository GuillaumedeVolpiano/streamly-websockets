-- |
-- Module      : Streamly.External.Network.WebSocket
-- Description : A WebSocket client over TCP
-- License     : BSD-3-Clause
--
-- A WebSocket client (RFC 6455) over plain TCP, written with Streamly.
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- > import Streamly.Data.Fold (drainMapM)
-- > import Streamly.Data.Stream (fold)
-- > import Streamly.Data.Stream qualified as S (take)
-- > import Streamly.External.Network.WebSocket (Message (TextMessage), config,
-- >                                             messages, send, withConnection)
-- >
-- > main :: IO ()
-- > main = withConnection (config "127.0.0.1" 9330 "/api") $ \connection -> do
-- >   send connection (TextMessage "hello")
-- >   fold (drainMapM print) (S.take 3 (messages connection))
--
-- Pings are answered, pongs counted and the closing handshake answered
-- while messages are received: a program that stops receiving stops all
-- three. Sending is safe from several threads; receiving is for one thread
-- at a time. TLS is not built in: 'withConnectionOver' takes a connection
-- opened by other means. There is no extension. For the side of a server,
-- see "Streamly.External.Network.WebSocket.Server".
module Streamly.External.Network.WebSocket
  ( -- * Connections
    Config (..)
  , config
  , Connection
  , withConnection
  , withConnectionOver

    -- * Messages
  , Message (..)
  , send
  , receive
  , messages

    -- * Pings and closing
  , ping
  , pongCount
  , close

    -- * Errors
  , WebSocketError (..)
  ) where

import Control.Exception (catch, finally, mask, throwIO)
import Data.ByteString (ByteString)
import Network.Socket (SocketOption (NoDelay), setSocketOption)
import Network.Socket qualified as N (close)
import Streamly.Data.Fold (drainMapM)
import Streamly.Data.Stream (fold)
import Streamly.Data.StreamK (fromStream)
import Streamly.External.Network.HTTP (HttpError (..), Transport (..),
                                       connectWithin, socketTransport)
import Streamly.External.Network.HTTP.Wire (Header, readResponseHeadWithin,
                                            renderRequest, validateRequest)
import Streamly.External.Network.WebSocket.Connection (Connection, Message (..),
                                                       close, established,
                                                       messages, ping,
                                                       pongCount, receive, send)
import Streamly.External.Network.WebSocket.Frame (Side (..),
                                                  WebSocketError (..))
import Streamly.External.Network.WebSocket.Handshake (checkUpgrade, newKey,
                                                      upgradeRequest)
import System.Timeout (timeout)

-- | Where to connect, and within what bounds.
data Config = Config
  { host :: String
  , port :: Int
  , path :: ByteString
  -- ^ The path and the query, such as @\/api@.
  , headers :: [Header]
  -- ^ Extra headers for the opening handshake.
  , maxMessageSize :: Int
  -- ^ The largest message accepted, in bytes. A larger one closes the
  -- connection with 'MessageTooLarge'.
  , openTimeout :: Maybe Int
  -- ^ Microseconds allowed for the connection to open: the TCP connection
  -- and the answer to the upgrade. 'Nothing' waits without end. Once the
  -- connection is open nothing limits the waits: a silent WebSocket is a
  -- normal one, and 'ping' with 'pongCount' tells a dead one.
  , closeTimeout :: Int
  -- ^ Microseconds to wait, once the function given to 'withConnection'
  -- is done, for the server to answer the closing of the connection.
  }
  deriving stock (Eq, Show)

-- | A host, a port and a path, with no extra header, messages of at most
-- 64 MiB, thirty seconds to open and one second to close.
config :: String -> Int -> ByteString -> Config
config host port path = Config {host, port, path, headers = [], maxMessageSize = 64 * 1024 * 1024, openTimeout = Just 30_000_000, closeTimeout = 1_000_000}

-- | Connect, open a WebSocket and use it. When the function returns, the
-- connection is closed in order: a close frame is sent, and the answer of
-- the server awaited for at most 'closeTimeout'. When it throws, the TCP
-- connection is closed at once.
--
-- Throws 'HandshakeFailed' when the server does not accept the upgrade,
-- 'OpenTimedOut' when the connection is not open in time,
-- 'Streamly.External.Network.HTTP.HttpError' (@InvalidRequest@) for a
-- path or a header that cannot be written, and 'IOError' when the TCP
-- connection fails.
withConnection :: Config -> (Connection -> IO a) -> IO a
withConnection settings use = do
  either throwIO pure (validateRequest (upgradeRequest settings.host settings.port settings.path settings.headers ""))
  -- The connection is opened unmasked, so that its time limit can stop
  -- it; connectWithin closes what it opened when it is stopped.
  mask $ \restore -> do
    socket <-
      restore (connectWithin settings.openTimeout settings.host settings.port)
        `catch` \case
          ConnectTimedOut -> throwIO OpenTimedOut
          other -> throwIO other
    ( restore $ do
        -- Frames are small and wait for answers: do not hold them back.
        setSocketOption socket NoDelay 1
        withConnectionOver (socketTransport socket) settings use
      )
      `finally` N.close socket

-- | Like 'withConnection', over a connection that is already open, such
-- as a TLS connection of @streamly-network-tls@ for a secure WebSocket
-- (@wss:\/\/@). The host and the port of the configuration only serve the
-- @Host@ header. Opening and closing the transport are up to the caller.
withConnectionOver :: Transport -> Config -> (Connection -> IO a) -> IO a
withConnectionOver transport settings use = do
  key <- newKey
  let request = upgradeRequest settings.host settings.port settings.path settings.headers key
  either throwIO pure (validateRequest request)
  rest <- opening $ do
    fold (drainMapM transport.send) (renderRequest request)
    (response, rest) <-
      -- No answer to an upgrade needs more than this.
      readResponseHeadWithin (Just 65536) (fromStream transport.receive)
        `catch` \(problem :: HttpError) -> throwIO (HandshakeFailed (show problem))
    either throwIO pure (checkUpgrade key response)
    pure rest
  established Client transport rest settings.maxMessageSize settings.closeTimeout use
  where
    opening action = case settings.openTimeout of
      Nothing -> action
      Just microseconds -> timeout microseconds action >>= maybe (throwIO OpenTimedOut) pure
