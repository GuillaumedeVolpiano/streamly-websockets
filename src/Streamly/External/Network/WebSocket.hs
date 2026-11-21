-- |
-- Module      : Streamly.External.Network.WebSocket
-- Description : A WebSocket client over TCP
-- License     : BSD-3-Clause
--
-- A WebSocket client (RFC 6455) over plain TCP, written with Streamly.
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- > import Streamly.Data.Fold qualified as Fold
-- > import Streamly.Data.Stream qualified as Stream
-- > import Streamly.External.Network.WebSocket
-- >
-- > main :: IO ()
-- > main = withConnection (config "127.0.0.1" 9330 "/api") $ \connection -> do
-- >   send connection (TextMessage "hello")
-- >   Stream.fold (Fold.drainMapM print) (Stream.take 3 (messages connection))
--
-- Pings are answered, pongs counted and the closing handshake answered
-- while messages are received: a program that stops receiving stops all
-- three. Sending is safe from several threads; receiving is for one thread
-- at a time. TLS is not built in: 'withConnectionOver' takes a connection
-- opened by other means. There is no extension.
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

import Control.Concurrent.MVar (MVar, modifyMVar, newMVar, withMVar)
import Control.Concurrent.STM (STM, TVar, atomically, modifyTVar', newTVarIO, readTVar)
import Control.Exception (SomeException, catch, finally, mask, onException, throwIO, try)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8', encodeUtf8)
import Data.Word (Word8)
import Network.Socket qualified as Net
import Streamly.Data.Array (Array)
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream
import Streamly.Data.StreamK (StreamK)
import Streamly.Data.StreamK qualified as StreamK
import Streamly.External.ByteString qualified as StreamlyBS
import Streamly.External.Network.HTTP (HttpError (..), Transport (..), connectWithin, socketTransport)
import Streamly.External.Network.HTTP.Wire (Header, readResponseHead, renderRequest, validateRequest)
import Streamly.External.Network.WebSocket.Frame
import Streamly.External.Network.WebSocket.Handshake
import System.Entropy (getEntropy)
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

-- | A whole message, put together from its frames.
data Message
  = TextMessage Text
  | BinaryMessage ByteString
  deriving stock (Eq, Show)

-- | An open connection.
data Connection = Connection
  { transport :: Transport
  , input :: MVar (StreamK IO (Array Word8))
  -- ^ What is still to read. Held while a message is received.
  , sendLock :: MVar ()
  -- ^ Held for each frame sent, and to read or change 'closeSent'.
  , closeSent :: IORef Bool
  , ended :: IORef Bool
  -- ^ Nothing more will be received.
  , pongs :: TVar Int
  , maxMessageSize :: Int
  }

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
        Net.setSocketOption socket Net.NoDelay 1
        withConnectionOver (socketTransport socket) settings use
      )
      `finally` Net.close socket

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
    Stream.fold (Fold.drainMapM transport.send) (renderRequest request)
    (response, rest) <-
      readResponseHead (StreamK.fromStream transport.receive)
        `catch` \(problem :: HttpError) -> throwIO (HandshakeFailed (show problem))
    either throwIO pure (checkUpgrade key response)
    pure rest
  connection <- Connection transport <$> newMVar rest <*> newMVar () <*> newIORef False <*> newIORef False <*> newTVarIO 0 <*> pure settings.maxMessageSize
  value <- use connection
  -- Wait for the close frame of the server before the connection goes:
  -- closing it at once can make the system reset it, and the server lose
  -- what it had not read yet.
  void . try @SomeException . timeout settings.closeTimeout $ do
    close connection
    Stream.fold Fold.drain (messages connection)
  pure value
  where
    opening action = case settings.openTimeout of
      Nothing -> action
      Just microseconds -> timeout microseconds action >>= maybe (throwIO OpenTimedOut) pure

-- | Send a message, in one frame.
--
-- Throws 'ConnectionClosed' once a close frame was sent or received, and
-- 'IOError' when the TCP connection fails.
send :: Connection -> Message -> IO ()
send connection = \case
  TextMessage text -> sendFrame connection False (Frame True TextFrame (encodeUtf8 text))
  BinaryMessage bytes -> sendFrame connection False (Frame True BinaryFrame bytes)

-- | Send a ping with a payload of at most 125 bytes. The pong that answers
-- it is counted by 'pongCount' when it is received.
--
-- Throws like 'send', and 'ProtocolViolation' for too long a payload.
ping :: Connection -> ByteString -> IO ()
ping connection payload = do
  when (BS.length payload > 125) (throwIO (ProtocolViolation "a ping payload longer than 125 bytes"))
  sendFrame connection False (Frame True PingFrame payload)

-- | The number of pongs received so far. It only grows while messages are
-- received. In 'STM', to wait for the next one.
pongCount :: Connection -> STM Int
pongCount connection = readTVar connection.pongs

-- | Start closing the connection, with the status 1000 (normal). Nothing
-- can be sent afterwards; messages can still be received until the server
-- answers. Does nothing when the connection is already closing.
close :: Connection -> IO ()
close connection = sendFrame connection True (Frame True CloseFrame (statusBytes 1000)) `catch` \case
  ConnectionClosed -> pure ()
  other -> throwIO other

-- | Send a frame under a fresh masking key. A closing frame marks the
-- connection as closing; nothing is sent after one.
sendFrame :: Connection -> Bool -> Frame -> IO ()
sendFrame connection closing frame =
  withMVar connection.sendLock $ \() -> do
    closed <- readIORef connection.closeSent
    when closed (throwIO ConnectionClosed)
    key <- getEntropy 4
    when closing (writeIORef connection.closeSent True)
    connection.transport.send (StreamlyBS.toArray (encodeFrame (Just key) frame))

-- | Like 'sendFrame', for answers that no longer matter once the
-- connection is closing or gone.
sendQuietly :: Connection -> Bool -> Frame -> IO ()
sendQuietly connection closing frame = void (try @SomeException (sendFrame connection closing frame))

-- | Receive the next message. 'Nothing' once the connection is closed, by
-- either side or by the loss of the TCP connection.
--
-- Meanwhile pings are answered, pongs counted, and a close frame of the
-- server answered.
--
-- Throws 'ProtocolViolation' or 'MessageTooLarge' when the server sends
-- what cannot be accepted (the connection is then closed), and 'IOError'
-- when the TCP connection fails.
receive :: Connection -> IO (Maybe Message)
receive connection = modifyMVar connection.input $ \input -> do
  over <- readIORef connection.ended
  if over
    then pure (input, Nothing)
    else assemble input Nothing `onException` writeIORef connection.ended True
  where
    finish = writeIORef connection.ended True
    -- Tell the server why, then give up.
    failWith :: Int -> WebSocketError -> IO a
    failWith status problem = do
      sendQuietly connection True (Frame True CloseFrame (statusBytes status))
      throwIO problem
    violation :: String -> IO a
    violation = failWith 1002 . ProtocolViolation
    -- The second argument holds the frames of a fragmented message read
    -- so far: its opcode, its payloads in reverse order and their size.
    assemble input pending = do
      next <-
        readFrame connection.maxMessageSize input `catch` \case
          problem@(MessageTooLarge _) -> failWith 1009 problem
          problem -> failWith 1002 problem
      case next of
        Nothing -> (StreamK.nil, Nothing) <$ finish
        Just (frame, rest) -> case frame.opcode of
          PingFrame -> sendQuietly connection False (Frame True PongFrame frame.payload) >> assemble rest pending
          PongFrame -> atomically (modifyTVar' connection.pongs (+ 1)) >> assemble rest pending
          CloseFrame -> do
            when (BS.length frame.payload == 1) (violation "a close frame with half a status")
            -- Answer with the same status, unless the closing is ours.
            sendQuietly connection True (Frame True CloseFrame (BS.take 2 frame.payload))
            (StreamK.nil, Nothing) <$ finish
          Continuation -> case pending of
            Nothing -> violation "a continuation frame without a message to continue"
            Just (opcode, parts, size) -> do
              let total = size + BS.length frame.payload
              when (total > connection.maxMessageSize) (failWith 1009 (MessageTooLarge connection.maxMessageSize))
              if frame.final
                then deliver rest opcode (BS.concat (reverse (frame.payload : parts)))
                else assemble rest (Just (opcode, frame.payload : parts, total))
          opcode -> do
            unless (pending == Nothing) (violation "a new message inside a fragmented one")
            if frame.final
              then deliver rest opcode frame.payload
              else assemble rest (Just (opcode, [frame.payload], BS.length frame.payload))
    deliver rest opcode bytes = case opcode of
      TextFrame -> case decodeUtf8' bytes of
        Right text -> pure (rest, Just (TextMessage text))
        Left _ -> failWith 1007 (ProtocolViolation "a text message that is not UTF-8")
      _ -> pure (rest, Just (BinaryMessage bytes))

-- | The messages received until the connection is closed. See 'receive'.
messages :: Connection -> Stream IO Message
messages connection = Stream.unfoldrM (\() -> fmap (,()) <$> receive connection) ()

-- | A close status as the first two bytes of the payload of a close frame.
statusBytes :: Int -> ByteString
statusBytes status = BS.pack [fromIntegral (status `div` 256), fromIntegral (status `mod` 256)]
