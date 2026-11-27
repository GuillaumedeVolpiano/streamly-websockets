-- |
-- Module      : Streamly.External.Network.WebSocket.Connection
-- Description : An open WebSocket, for a client as for a server
-- License     : BSD-3-Clause
--
-- What both sides do once the opening handshake is over: sending and
-- receiving messages, pings, and the closing handshake. The client and the
-- server modules open connections and export these functions.
module Streamly.External.Network.WebSocket.Connection
  ( Connection
  , established
  , Message (..)
  , send
  , receive
  , messages
  , ping
  , pongCount
  , close
  ) where

import Control.Concurrent.MVar (MVar, modifyMVar, newMVar, withMVar)
import Control.Concurrent.STM (STM, TVar, atomically, modifyTVar', newTVarIO,
                               readTVar)
import Control.Exception (SomeException, catch, onException, throwIO, try)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString, pack)
import Data.ByteString qualified as BS (concat, length, take)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8', encodeUtf8)
import Data.Word (Word8)
import Streamly.Data.Array (Array)
import Streamly.Data.Fold (drain)
import Streamly.Data.Stream (Stream, fold, unfoldrM)
import Streamly.Data.StreamK (StreamK, nil)
import Streamly.External.ByteString (toArray)
import Streamly.External.Network.HTTP (Transport (..))
import Streamly.External.Network.WebSocket.Frame (Frame (..),
                                                  Opcode (BinaryFrame, CloseFrame, Continuation, PingFrame, PongFrame, TextFrame),
                                                  Side (..),
                                                  WebSocketError (ConnectionClosed, MessageTooLarge, ProtocolViolation),
                                                  encodeFrame, readFrame)
import System.Entropy (getEntropy)
import System.Timeout (timeout)

-- | A whole message, put together from its frames.
data Message
  = TextMessage Text
  | BinaryMessage ByteString
  deriving stock (Eq, Show)

-- | An open connection.
data Connection = Connection
  { side :: Side
  , transport :: Transport
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

-- | Use a connection whose opening handshake is over: the side this is,
-- the transport, what was received after the handshake, the largest
-- message accepted and the microseconds to wait for the closing. When the
-- function returns, the connection is closed in order: a close frame is
-- sent, and the answer of the other side awaited.
established :: Side -> Transport -> StreamK IO (Array Word8) -> Int -> Int -> (Connection -> IO a) -> IO a
established side transport rest maxMessageSize closeTimeout use = do
  connection <- Connection side transport <$> newMVar rest <*> newMVar () <*> newIORef False <*> newIORef False <*> newTVarIO 0 <*> pure maxMessageSize
  value <- use connection
  -- Wait for the close frame of the other side before the connection
  -- goes: closing it at once can make the system reset it, and the other
  -- side lose what it had not read yet.
  void . try @SomeException . timeout closeTimeout $ do
    close connection
    fold drain (messages connection)
  pure value

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
-- Throws like @send@, and 'ProtocolViolation' for too long a payload.
ping :: Connection -> ByteString -> IO ()
ping connection payload = do
  when (BS.length payload > 125) (throwIO (ProtocolViolation "a ping payload longer than 125 bytes"))
  sendFrame connection False (Frame True PingFrame payload)

-- | The number of pongs received so far. It only grows while messages are
-- received. In 'STM', to wait for the next one.
pongCount :: Connection -> STM Int
pongCount connection = readTVar connection.pongs

-- | Start closing the connection, with the status 1000 (normal). Nothing
-- can be sent afterwards; messages can still be received until the other
-- side answers. Does nothing when the connection is already closing.
close :: Connection -> IO ()
close connection = sendFrame connection True (Frame True CloseFrame (statusBytes 1000)) `catch` \case
  ConnectionClosed -> pure ()
  other -> throwIO other

-- | Send a frame, under a fresh masking key for a client. A closing frame marks the
-- connection as closing; nothing is sent after one.
sendFrame :: Connection -> Bool -> Frame -> IO ()
sendFrame connection closing frame =
  withMVar connection.sendLock $ \() -> do
    closed <- readIORef connection.closeSent
    when closed (throwIO ConnectionClosed)
    -- A client masks what it sends; a server does not.
    key <- case connection.side of
      Client -> Just <$> getEntropy 4
      Server -> pure Nothing
    when closing (writeIORef connection.closeSent True)
    connection.transport.send (toArray (encodeFrame key frame))

-- | Like 'sendFrame', for answers that no longer matter once the
-- connection is closing or gone.
sendQuietly :: Connection -> Bool -> Frame -> IO ()
sendQuietly connection closing frame = void (try @SomeException (sendFrame connection closing frame))

-- | Receive the next message. 'Nothing' once the connection is closed, by
-- either side or by the loss of the TCP connection.
--
-- Meanwhile pings are answered, pongs counted, and a close frame of the
-- other side answered.
--
-- Throws 'ProtocolViolation' or 'MessageTooLarge' when the other side
-- sends what cannot be accepted (the connection is then closed), and 'IOError'
-- when the TCP connection fails.
receive :: Connection -> IO (Maybe Message)
receive connection = modifyMVar connection.input $ \input -> do
  over <- readIORef connection.ended
  if over
    then pure (input, Nothing)
    else assemble input Nothing `onException` writeIORef connection.ended True
  where
    finish = writeIORef connection.ended True
    -- Tell the other side why, then give up.
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
        readFrame connection.side connection.maxMessageSize input `catch` \case
          problem@(MessageTooLarge _) -> failWith 1009 problem
          problem -> failWith 1002 problem
      case next of
        Nothing -> (nil, Nothing) <$ finish
        Just (frame, rest) -> case frame.opcode of
          PingFrame -> sendQuietly connection False (Frame True PongFrame frame.payload) >> assemble rest pending
          PongFrame -> atomically (modifyTVar' connection.pongs (+ 1)) >> assemble rest pending
          CloseFrame -> do
            when (BS.length frame.payload == 1) (violation "a close frame with half a status")
            -- Answer with the same status, unless the closing is ours.
            sendQuietly connection True (Frame True CloseFrame (BS.take 2 frame.payload))
            (nil, Nothing) <$ finish
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

-- | The messages received until the connection is closed. See @receive@.
messages :: Connection -> Stream IO Message
messages connection = unfoldrM (\() -> fmap (,()) <$> receive connection) ()

-- | A close status as the first two bytes of the payload of a close frame.
statusBytes :: Int -> ByteString
statusBytes status = pack [fromIntegral (status `div` 256), fromIntegral (status `mod` 256)]
