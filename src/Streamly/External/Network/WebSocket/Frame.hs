-- |
-- Module      : Streamly.External.Network.WebSocket.Frame
-- Description : WebSocket frames, written and read
-- License     : BSD-3-Clause
--
-- The frames of RFC 6455, section 5: writing them, masked or not, and
-- reading them from a stream of byte arrays, on either side.
module Streamly.External.Network.WebSocket.Frame
  ( -- * Frames
    Frame (..)
  , Opcode (..)
  , isControl
  , encodeFrame
  , applyMask

    -- * Reading
  , Side (..)
  , FrameHeader (..)
  , frameHeader
  , readFrame

    -- * Errors
  , WebSocketError (..)
  ) where

import Control.Exception (Exception, throwIO)
import Control.Monad (when)
import Data.Bits (shiftR, testBit, xor, (.&.), (.|.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Unsafe qualified as BS (unsafeIndex)
import Data.Word (Word64, Word8)
import Streamly.Data.Array (Array)
import Streamly.Data.Array qualified as Array
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Parser (Parser)
import Streamly.Data.Parser qualified as Parser
import Streamly.Data.StreamK (StreamK)
import Streamly.Data.StreamK qualified as StreamK
import Streamly.External.ByteString qualified as StreamlyBS

-- | What a frame carries.
data Opcode
  = -- | The next part of a fragmented message.
    Continuation
  | TextFrame
  | BinaryFrame
  | CloseFrame
  | PingFrame
  | PongFrame
  deriving stock (Eq, Show)

-- | Close, ping and pong: frames about the connection itself.
isControl :: Opcode -> Bool
isControl opcode = opcode `elem` [CloseFrame, PingFrame, PongFrame]

-- | A frame, its payload unmasked.
data Frame = Frame
  { final :: Bool
  -- ^ The last frame of its message.
  , opcode :: Opcode
  , payload :: ByteString
  }
  deriving stock (Eq, Show)

-- | What can go wrong on a WebSocket connection. Failures of the TCP
-- connection itself stay 'IOError's.
data WebSocketError
  = -- | The opening handshake failed: the server did not accept the upgrade
    -- or, for a server, the client did not ask for one properly. Why.
    HandshakeFailed String
  | -- | The other side broke the protocol: how.
    ProtocolViolation String
  | -- | A frame or a message larger than the limit of the connection, in
    -- bytes: the limit.
    MessageTooLarge Int
  | -- | The connection was not open within the time allowed: the server
    -- did not accept it, or did not answer the upgrade.
    OpenTimedOut
  | -- | Sending on a connection that is closed or closing.
    ConnectionClosed
  deriving stock (Eq, Show)

instance Exception WebSocketError

-- | The bytes of a frame. With a masking key of four bytes the payload is
-- masked, as a client must do; without one it is sent as it is, as a
-- server does.
encodeFrame :: Maybe ByteString -> Frame -> ByteString
encodeFrame mask frame =
  BS.concat $
    [BS.pack (first : lengthBytes)]
      <> maybe [frame.payload] (\key -> [key, applyMask key frame.payload]) mask
  where
    first = (if frame.final then 0x80 else 0) .|. opcodeNumber frame.opcode
    maskBit = maybe 0 (const 0x80) mask
    size = BS.length frame.payload
    lengthBytes
      | size < 126 = [maskBit .|. fromIntegral size]
      | size < 65536 = (maskBit .|. 126) : bigEndian 2
      | otherwise = (maskBit .|. 127) : bigEndian 8
    bigEndian :: Int -> [Word8]
    bigEndian count = [fromIntegral (size `shiftR` (8 * index)) | index <- [count - 1, count - 2 .. 0]]

opcodeNumber :: Opcode -> Word8
opcodeNumber = \case
  Continuation -> 0
  TextFrame -> 1
  BinaryFrame -> 2
  CloseFrame -> 8
  PingFrame -> 9
  PongFrame -> 10

-- | Mask or unmask a payload with a key of four bytes: each byte is
-- combined by exclusive or with the byte of the key at its position modulo
-- four.
applyMask :: ByteString -> ByteString -> ByteString
applyMask key
  | BS.length key /= 4 = id
  | otherwise = snd . BS.mapAccumL (\index byte -> (index + 1, byte `xor` BS.unsafeIndex key (index .&. 3))) (0 :: Int)

-- | What precedes the payload of a frame.
data FrameHeader = FrameHeader
  { final :: Bool
  , opcode :: Opcode
  , mask :: Maybe ByteString
  -- ^ The masking key, of four bytes, when the payload is masked.
  , size :: Int
  -- ^ The number of bytes of the payload.
  }
  deriving stock (Eq, Show)

-- | The header of a frame. Refuses reserved bits (no extension is
-- negotiated), unknown opcodes, lengths not written in the shortest form,
-- and control frames that are fragmented or longer than 125 bytes.
frameHeader :: Monad m => Parser Word8 m FrameHeader
frameHeader = do
  first <- Parser.one
  when (first .&. 0x70 /= 0) (Parser.die "a reserved bit is set")
  opcode <- case first .&. 0x0F of
    0 -> pure Continuation
    1 -> pure TextFrame
    2 -> pure BinaryFrame
    8 -> pure CloseFrame
    9 -> pure PingFrame
    10 -> pure PongFrame
    other -> Parser.die ("unknown opcode " <> show other)
  second <- Parser.one
  let final = testBit first 7
  size <- case second .&. 0x7F of
    126 -> do
      long <- bigEndian 2
      when (long < 126) (Parser.die "a length not in its shortest form")
      pure (fromIntegral long)
    127 -> do
      long <- bigEndian 8
      when (long < 65536) (Parser.die "a length not in its shortest form")
      when (long > fromIntegral (maxBound :: Int)) (Parser.die "a length beyond what can be held")
      pure (fromIntegral long)
    short -> pure (fromIntegral short)
  when (isControl opcode && not final) (Parser.die "a fragmented control frame")
  when (isControl opcode && size > 125) (Parser.die "a control frame longer than 125 bytes")
  mask <- if testBit second 7 then Just . BS.pack <$> Parser.takeEQ 4 Fold.toList else pure Nothing
  pure FrameHeader {final, opcode, mask, size}
  where
    bigEndian :: Monad m => Int -> Parser Word8 m Word64
    bigEndian count = foldl' (\total byte -> total * 256 + fromIntegral byte) 0 <$> Parser.takeEQ count Fold.toList

-- | The two ends of a connection. A client masks the frames it sends; a
-- server does not.
data Side = Client | Server
  deriving stock (Eq, Show)

-- | Read one frame as the given side receives it, with the largest payload
-- accepted, and give the input that follows it. 'Nothing' when the input
-- has ended before the frame starts. A payload masked by a client is
-- given unmasked.
--
-- Throws 'ProtocolViolation' for a header that cannot be read, for a
-- frame masked when it must not be (those a client receives) or not
-- masked when it must be (those a server receives), and for an input that
-- ends inside the frame; 'MessageTooLarge' for a payload beyond the limit.
readFrame :: Side -> Int -> StreamK IO (Array Word8) -> IO (Maybe (Frame, StreamK IO (Array Word8)))
readFrame side limit input =
  StreamK.uncons input >>= \case
    Nothing -> pure Nothing
    Just (array, rest)
      | Array.length array == 0 -> readFrame side limit rest
      | otherwise -> do
          (outcome, afterHeader) <- Array.parseBreak (Array.toParserK frameHeader) (StreamK.cons array rest)
          found <- case outcome of
            Left (Parser.ParseError problem) -> throwIO (ProtocolViolation problem)
            Right found -> pure found
          case (side, found.mask) of
            (Client, Just _) -> throwIO (ProtocolViolation "a masked frame from the server")
            (Server, Nothing) -> throwIO (ProtocolViolation "a frame from the client that is not masked")
            _ -> pure ()
          when (found.size > limit) (throwIO (MessageTooLarge limit))
          takeBytes found.size afterHeader >>= \case
            Nothing -> throwIO (ProtocolViolation "the connection ended inside a frame")
            Just (received, afterFrame) ->
              let payload = maybe received (`applyMask` received) found.mask
               in pure (Just (Frame {final = found.final, opcode = found.opcode, payload}, afterFrame))

-- | Take a number of bytes from the input, without copying the arrays
-- that hold them. 'Nothing' when the input ends first.
takeBytes :: Int -> StreamK IO (Array Word8) -> IO (Maybe (ByteString, StreamK IO (Array Word8)))
takeBytes = go []
  where
    go taken 0 input = pure (Just (BS.concat (reverse taken), input))
    go taken remaining input =
      StreamK.uncons input >>= \case
        Nothing -> pure Nothing
        Just (array, rest)
          | BS.length bytes <= remaining -> go (bytes : taken) (remaining - BS.length bytes) rest
          | otherwise ->
              let (front, back) = BS.splitAt remaining bytes
               in pure (Just (BS.concat (reverse (front : taken)), StreamK.cons (StreamlyBS.toArray back) rest))
          where
            bytes = StreamlyBS.fromArray array
