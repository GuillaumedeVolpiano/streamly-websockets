-- |
-- Module      : Streamly.External.Network.WebSocket.Handshake
-- Description : The HTTP exchange that opens a WebSocket
-- License     : BSD-3-Clause
--
-- The opening handshake of RFC 6455, section 4. From the client: an HTTP
-- request that asks for the upgrade, and the check of the answer. From the
-- server: the check of the request, and the answer.
module Streamly.External.Network.WebSocket.Handshake
  ( newKey
  , acceptFor
  , upgradeRequest
  , checkUpgrade

    -- * The side of a server
  , checkUpgradeRequest
  , upgradeResponse
  , refusal
  ) where

import Control.Monad (unless, when)
import Crypto.Hash.SHA1 (hash)
import Data.ByteString (ByteString, split)
import Data.ByteString qualified as BS (null)
import Data.ByteString.Base64 (encode)
import Data.ByteString.Char8 (strip, unpack)
import Data.ByteString.Char8 qualified as BS8 (map)
import Data.Char (toLower)
import Data.Maybe (isJust)
import Streamly.External.Network.HTTP.Wire (Header, Request (..),
                                            RequestBody (..), RequestHead,
                                            Response (..), header,
                                            requestHeader, requestMethod)
import Streamly.External.Network.WebSocket.Frame (WebSocketError (..))
import System.Entropy (getEntropy)

-- | A key for one handshake: sixteen random bytes in base64.
newKey :: IO ByteString
newKey = encode <$> getEntropy 16

-- | What a server must answer in @Sec-WebSocket-Accept@ to a key.
acceptFor :: ByteString -> ByteString
acceptFor key = encode (hash (key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))

-- | The request that asks a host and a port to open a WebSocket on a
-- path, with extra headers and the key of the handshake.
upgradeRequest :: String -> Int -> ByteString -> [Header] -> ByteString -> Request
upgradeRequest host port path extra key =
  Request
    { method = "GET"
    , host
    , port
    , target = path
    , headers =
        [ ("Upgrade", "websocket")
        , ("Connection", "Upgrade")
        , ("Sec-WebSocket-Key", key)
        , ("Sec-WebSocket-Version", "13")
        ]
          <> extra
    , body = NoBody
    }

-- | Check that the answer accepts the upgrade asked with a key: status
-- 101, the upgrade to @websocket@, the proof that the key was read, and
-- no extension, since none was offered.
checkUpgrade :: ByteString -> Response body -> Either WebSocketError ()
checkUpgrade key response = do
  unless (response.status == 101) $
    refuse ("the server answered " <> show response.status <> " " <> unpack response.reason)
  unless (fmap lower (header "upgrade" response) == Just "websocket") $
    refuse "the answer does not upgrade to websocket"
  unless (maybe False (elem "upgrade" . tokens) (header "connection" response)) $
    refuse "the answer does not say Connection: Upgrade"
  unless (header "sec-websocket-accept" response == Just (acceptFor key)) $
    refuse "the answer does not prove that the key was read"
  when (isJust (header "sec-websocket-extensions" response)) $
    refuse "the server uses an extension that was not offered"
  where
    refuse = Left . HandshakeFailed
    lower = BS8.map toLower
    tokens = map (lower . strip) . split 44

-- | Check that a request asks properly for a WebSocket, and give its key:
-- a @GET@ that upgrades to @websocket@, version 13, with a key.
checkUpgradeRequest :: RequestHead -> Either WebSocketError ByteString
checkUpgradeRequest request = do
  unless (requestMethod request == "GET") (refuse "the request is not a GET")
  unless (fmap lower (requestHeader "upgrade" request) == Just "websocket") (refuse "the request does not ask for websocket")
  unless (maybe False (elem "upgrade" . tokens) (requestHeader "connection" request)) (refuse "the request does not say Connection: Upgrade")
  unless (fmap strip (requestHeader "sec-websocket-version" request) == Just "13") (refuse "the request is not for version 13")
  case requestHeader "sec-websocket-key" request of
    Just key | not (BS.null key) -> Right key
    _ -> refuse "the request has no key"
  where
    refuse = Left . HandshakeFailed
    lower = BS8.map toLower
    tokens = map (lower . strip) . split 44

-- | The answer that accepts the upgrade asked with a key.
upgradeResponse :: ByteString -> ByteString
upgradeResponse key =
  "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " <> acceptFor key <> "\r\n\r\n"

-- | The answer to a request that does not ask properly for a WebSocket.
refusal :: ByteString
refusal = "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nSec-WebSocket-Version: 13\r\nContent-Length: 0\r\n\r\n"
