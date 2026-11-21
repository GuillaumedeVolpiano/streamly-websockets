-- |
-- Module      : Streamly.External.Network.WebSocket.Handshake
-- Description : The HTTP exchange that opens a WebSocket
-- License     : BSD-3-Clause
--
-- The opening handshake of RFC 6455, section 4, seen from the client: an
-- HTTP request that asks for the upgrade, and the check of the answer.
module Streamly.External.Network.WebSocket.Handshake
  ( newKey
  , acceptFor
  , upgradeRequest
  , checkUpgrade
  ) where

import Control.Monad (unless, when)
import Crypto.Hash.SHA1 qualified as SHA1
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base64 qualified as Base64
import Data.ByteString.Char8 qualified as BS8
import Data.Char (toLower)
import Data.Maybe (isJust)
import Streamly.External.Network.HTTP.Wire (Header, Request (..), RequestBody (..), Response (..), header)
import Streamly.External.Network.WebSocket.Frame (WebSocketError (..))
import System.Entropy (getEntropy)

-- | A key for one handshake: sixteen random bytes in base64.
newKey :: IO ByteString
newKey = Base64.encode <$> getEntropy 16

-- | What a server must answer in @Sec-WebSocket-Accept@ to a key.
acceptFor :: ByteString -> ByteString
acceptFor key = Base64.encode (SHA1.hash (key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))

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
    refuse ("the server answered " <> show response.status <> " " <> BS8.unpack response.reason)
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
    tokens = map (lower . BS8.strip) . BS.split 44
