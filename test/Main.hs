-- | Frames on byte arrays cut anywhere; the handshake; then the client
-- against a server of the @websockets@ package, and against servers that
-- send chosen bytes.
module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, wait, withAsync)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (atomically, check)
import Control.Exception (bracket, try)
import Control.Monad (forever, void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Text qualified as Text
import Network.Socket qualified as Net
import Network.Socket.ByteString qualified as NetBS
import Network.WebSockets qualified as WS
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream
import Streamly.Data.StreamK qualified as StreamK
import Streamly.External.ByteString qualified as StreamlyBS
import Streamly.External.Network.HTTP (HttpError (..), connect, socketTransport)
import Streamly.External.Network.WebSocket
import Streamly.External.Network.WebSocket.Frame
import Streamly.External.Network.WebSocket.Handshake (acceptFor)
import Streamly.External.Network.WebSocket.Server qualified as Server
import Test.Tasty (TestTree, defaultMain, localOption, mkTimeout, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))
import Test.Tasty.QuickCheck (Positive (..), ioProperty, testProperty, (===))

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  localOption (mkTimeout 20_000_000) $
    testGroup
      "streamly-websockets"
      [ testGroup "frames" frames
      , testGroup "against the websockets package" interop
      , testGroup "against chosen bytes" chosen
      , testGroup "the server against the websockets package" serving
      , testGroup "the server against the client, and against chosen bytes" serverSide
      ]

pieces :: Int -> ByteString -> [ByteString]
pieces size bytes
  | BS.null bytes = []
  | otherwise = let (front, back) = BS.splitAt size bytes in front : pieces size back

-- | Read every frame from bytes cut into pieces of a size.
readAll :: Int -> Int -> ByteString -> IO [Frame]
readAll limit size bytes = go (StreamK.fromStream (Stream.fromList (map StreamlyBS.toArray (pieces size bytes))))
  where
    go input =
      readFrame Client limit input >>= \case
        Nothing -> pure []
        Just (frame, rest) -> (frame :) <$> go rest

violates :: IO a -> IO ()
violates action =
  try action >>= \case
    Left (ProtocolViolation _) -> pure ()
    Left other -> assertFailure ("expected ProtocolViolation, got " <> show other)
    Right _ -> assertFailure "no error, expected ProtocolViolation"

frames :: [TestTree]
frames =
  [ testCase "the examples of RFC 6455, section 5.7" $ do
      encodeFrame Nothing (Frame True TextFrame "Hello") @?= BS.pack [0x81, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f]
      encodeFrame (Just (BS.pack [0x37, 0xfa, 0x21, 0x3d])) (Frame True TextFrame "Hello")
        @?= BS.pack [0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58]
      encodeFrame Nothing (Frame False TextFrame "Hel") @?= BS.pack [0x01, 0x03, 0x48, 0x65, 0x6c]
      encodeFrame Nothing (Frame True Continuation "lo") @?= BS.pack [0x80, 0x02, 0x6c, 0x6f]
      encodeFrame Nothing (Frame True PingFrame "Hello") @?= BS.pack [0x89, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f]
      BS.take 4 (encodeFrame Nothing (Frame True BinaryFrame (BS.replicate 256 0))) @?= BS.pack [0x82, 0x7E, 0x01, 0x00]
      BS.take 10 (encodeFrame Nothing (Frame True BinaryFrame (BS.replicate 65536 0)))
        @?= BS.pack [0x82, 0x7F, 0, 0, 0, 0, 0, 0x01, 0, 0]
  , testProperty "masking twice gives the payload back" $ \bytes ->
      let payload = BS.pack bytes
          key = BS.pack [1, 2, 254, 255]
       in applyMask key (applyMask key payload) === payload
  , testProperty "frames are read back however the bytes are cut" $ \(Positive size) ->
      ioProperty $ do
        let sent =
              [ Frame False TextFrame "Hel"
              , Frame True PingFrame ""
              , Frame True Continuation "lo"
              , Frame True BinaryFrame (BS.replicate 125 1)
              , Frame True BinaryFrame (BS.replicate 126 2)
              , Frame True BinaryFrame (BS.replicate 70_000 3)
              , Frame True CloseFrame "\3\232bye"
              ]
        received <- readAll 100_000 (size * 7) (BS.concat (map (encodeFrame Nothing) sent))
        pure (received === sent)
  , testCase "no frame in no input" $
      readAll 100 4096 "" >>= (@?= [])
  , testCase "a payload beyond the limit" $
      try (readAll 99 4096 (encodeFrame Nothing (Frame True BinaryFrame (BS.replicate 100 0)))) >>= (@?= Left (MessageTooLarge 99))
  , testCase "a masked frame, as a server must not send" $
      violates (readAll 100 4096 (encodeFrame (Just "abcd") (Frame True TextFrame "Hello")))
  , testCase "input that ends inside a header or a payload" $ do
      violates (readAll 100 4096 (BS.pack [0x81]))
      violates (readAll 1000 4096 (BS.pack [0x82, 0x7E, 0x01]))
      violates (readAll 100 4096 (BS.pack [0x81, 0x05, 0x48, 0x65]))
  , testCase "reserved bits, unknown opcodes, lengths not in their shortest form" $ do
      violates (readAll 100 4096 (BS.pack [0xC1, 0x00]))
      violates (readAll 100 4096 (BS.pack [0x83, 0x00]))
      violates (readAll 100 4096 (BS.pack [0x82, 0x7E, 0x00, 0x7D] <> BS.replicate 125 0))
      violates (readAll 100_000 4096 (BS.pack [0x82, 0x7F, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF] <> BS.replicate 65535 0))
      violates (readAll 100 4096 (BS.pack [0x82, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0]))
  , testProperty "a server reads the masked frames of a client, however the bytes are cut" $ \(Positive size) ->
      ioProperty $ do
        let sent = [Frame False TextFrame "Hel", Frame True Continuation "lo", Frame True BinaryFrame (BS.replicate 70_000 3), Frame True CloseFrame "\3\232"]
            go input =
              readFrame Server 100_000 input >>= \case
                Nothing -> pure []
                Just (frame, rest) -> (frame :) <$> go rest
        received <- go (StreamK.fromStream (Stream.fromList (map StreamlyBS.toArray (pieces (size * 7) (BS.concat (map (encodeFrame (Just "\1\2\254\255")) sent))))))
        pure (received === sent)
  , testCase "a server refuses a frame that is not masked" $
      violates (readFrame Server 100 (StreamK.fromStream (Stream.fromList [StreamlyBS.toArray (encodeFrame Nothing (Frame True TextFrame "Hello"))])))
  , testCase "control frames fragmented or too long" $ do
      violates (readAll 100 4096 (BS.pack [0x09, 0x00]))
      violates (readAll 1000 4096 (BS.pack [0x89, 0x7E, 0x00, 0x7E] <> BS.replicate 126 0))
  ]

-- | A socket listening on a free port of the loopback interface.
listening :: IO Net.Socket
listening = do
  listener <- Net.socket Net.AF_INET Net.Stream Net.defaultProtocol
  Net.setSocketOption listener Net.ReuseAddr 1
  Net.bind listener (Net.SockAddrInet 0 (Net.tupleToHostAddress (127, 0, 0, 1)))
  Net.listen listener 1
  pure listener

-- | Serve one connection with the @websockets@ package, and run a client
-- against it. Returns what both return.
withPeer :: (WS.Connection -> IO a) -> (Config -> IO b) -> IO (a, b)
withPeer serve run =
  bracket listening Net.close $ \listener -> do
    port <- Net.socketPort listener
    let accepted = bracket (fst <$> Net.accept listener) Net.close $ \peer -> do
          pending <- WS.makePendingConnection peer WS.defaultConnectionOptions
          WS.acceptRequest pending >>= serve
    withAsync accepted $ \server -> do
      result <- run (config "127.0.0.1" (fromIntegral port) "/chat?room=1")
      served <- wait server
      pure (served, result)

-- | Echo data messages until the client closes; 'True' when it closed in
-- order.
echo :: WS.Connection -> IO Bool
echo connection =
  try (forever (WS.receiveDataMessage connection >>= WS.sendDataMessage connection)) >>= \case
    Left (WS.CloseRequest 1000 _) -> pure True
    Left _ -> pure False
    Right () -> pure False

interop :: [TestTree]
interop =
  [ testCase "text and binary messages come back, and the connection closes in order" $ do
      (closedInOrder, received) <- withPeer echo $ \settings -> withConnection settings $ \connection -> do
        send connection (TextMessage "héllo")
        send connection (BinaryMessage (BS.pack [0, 1, 255]))
        send connection (TextMessage "")
        Stream.toList (Stream.take 3 (messages connection))
      received @?= [TextMessage "héllo", BinaryMessage (BS.pack [0, 1, 255]), TextMessage ""]
      closedInOrder @?= True
  , testCase "over a transport of the caller" $ do
      (closedInOrder, received) <- withPeer echo $ \settings ->
        bracket (connect settings.host settings.port) Net.close $ \socket ->
          withConnectionOver (socketTransport socket) settings $ \connection -> do
            send connection (TextMessage "over a transport")
            receive connection
      (received, closedInOrder) @?= (Just (TextMessage "over a transport"), True)
  , testCase "messages of every length form" $ do
      let sizes = [125, 126, 65535, 65536, 300_000]
      (_, received) <- withPeer echo $ \settings -> withConnection settings $ \connection -> do
        mapM_ (\size -> send connection (BinaryMessage (BS.replicate size 7))) sizes
        Stream.toList (Stream.take (length sizes) (messages connection))
      map (\case BinaryMessage bytes -> BS.length bytes; TextMessage text -> negate (Text.length text)) received @?= sizes
  , testCase "the path and the extra headers reach the server" $ do
      let serve listener = bracket (fst <$> Net.accept listener) Net.close $ \peer -> do
            pending <- WS.makePendingConnection peer WS.defaultConnectionOptions
            let request = WS.pendingRequest pending
            void (WS.acceptRequest pending)
            pure (WS.requestPath request, lookup "X-Extra" (WS.requestHeaders request), lookup "Host" (WS.requestHeaders request))
      bracket listening Net.close $ \listener -> do
        port <- Net.socketPort listener
        withAsync (serve listener) $ \server -> do
          withConnection (config "127.0.0.1" (fromIntegral port) "/chat?room=1") {headers = [("X-Extra", "yes")]} (\_ -> pure ())
          seen <- wait server
          seen @?= ("/chat?room=1", Just "yes", Just ("127.0.0.1:" <> BS8.pack (show port)))
  , testCase "a ping of the server is answered while receiving" $ do
      let serve connection = do
            WS.sendPing connection ("are you there" :: ByteString)
            answer <- WS.receive connection
            WS.sendTextData connection ("done" :: ByteString)
            void (try @WS.ConnectionException (forever (WS.receiveDataMessage connection)))
            pure answer
      (answer, received) <- withPeer serve $ \settings -> withConnection settings receive
      received @?= Just (TextMessage "done")
      answer @?= WS.ControlMessage (WS.Pong "are you there")
  , testCase "the pong to a ping is counted" $ do
      (_, counts) <- withPeer echo $ \settings -> withConnection settings $ \connection -> do
        before <- atomically (pongCount connection)
        ping connection "one"
        send connection (TextMessage "after the ping")
        message <- receive connection
        after <- atomically (pongCount connection)
        pure (before, message, after)
      counts @?= (0, Just (TextMessage "after the ping"), 1)
  , testCase "waiting for a pong in STM, with another thread receiving" $ do
      (_, arrived) <- withPeer echo $ \settings -> withConnection settings $ \connection ->
        withAsync (Stream.toList (messages connection)) $ \_ -> do
          ping connection ""
          atomically (pongCount connection >>= check . (> 0))
          pure True
      arrived @?= True
  , testCase "a server that closes ends the messages, and gets its close back" $ do
      let serve connection = do
            WS.sendTextData connection ("last" :: ByteString)
            WS.sendClose connection ("bye" :: ByteString)
            try (forever (WS.receiveDataMessage connection)) >>= \case
              Left (WS.CloseRequest status _) -> pure (Just status)
              Left _ -> pure Nothing
              Right () -> pure Nothing
      (echoed, outcome) <- withPeer serve $ \settings -> withConnection settings $ \connection -> do
        received <- Stream.toList (messages connection)
        again <- receive connection
        late <- try (send connection (TextMessage "too late"))
        pure (received, again, late)
      outcome @?= ([TextMessage "last"], Nothing, Left ConnectionClosed)
      echoed @?= Just 1000
  , testCase "after close nothing is sent, and messages still arrive" $ do
      let serve connection = do
            first <- WS.receiveDataMessage connection
            WS.sendTextData connection ("still here" :: ByteString)
            closed <- try @WS.ConnectionException (WS.receiveDataMessage connection)
            pure (first, either (const True) (const False) closed)
      ((first, closed), outcome) <- withPeer serve $ \settings -> withConnection settings $ \connection -> do
        send connection (TextMessage "hello")
        close connection
        close connection
        late <- try (send connection (TextMessage "too late"))
        received <- Stream.toList (messages connection)
        pure (late, received)
      outcome @?= (Left ConnectionClosed, [TextMessage "still here"])
      first @?= WS.Text (LazyByteString.fromStrict "hello") Nothing
      closed @?= True
  ]

-- | A server that reads the opening request, answers it with a function
-- of the key sent, and ends its side of the connection. Returns the request with the result.
withRawServer :: (ByteString -> [ByteString]) -> (Config -> IO a) -> IO (ByteString, a)
withRawServer answer run =
  bracket listening Net.close $ \listener -> do
    port <- Net.socketPort listener
    let serve = bracket (fst <$> Net.accept listener) Net.close $ \peer -> do
          request <- readHead peer ""
          mapM_ (NetBS.sendAll peer) (answer (keyOf request))
          -- Say that nothing more comes, then read until the client is
          -- done, so that nothing is reset.
          Net.shutdown peer Net.ShutdownSend
          drainFrom peer
          pure request
    withAsync serve $ \server -> do
      result <- run (config "127.0.0.1" (fromIntegral port) "/")
      request <- wait server
      pure (request, result)
  where
    readHead peer seen
      | "\r\n\r\n" `BS.isInfixOf` seen = pure seen
      | otherwise = do
          more <- NetBS.recv peer 4096
          if BS.null more then pure seen else readHead peer (seen <> more)
    drainFrom peer = do
      more <- NetBS.recv peer 4096
      if BS.null more then pure () else drainFrom peer
    keyOf request =
      BS8.strip . BS.takeWhile (/= 13) . BS.drop 18 . snd $ BS.breakSubstring "Sec-WebSocket-Key:" request

-- | The answer that accepts the upgrade, then these frames.
accepting :: [Frame] -> ByteString -> [ByteString]
accepting sent key =
  ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: WebSocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Accept: " <> acceptFor key <> "\r\n\r\n")
    : map (encodeFrame Nothing) sent

refused :: (ByteString -> [ByteString]) -> IO ()
refused answer =
  try (withRawServer answer (\settings -> withConnection settings (\_ -> pure ()))) >>= \case
    Left (HandshakeFailed _) -> pure ()
    Left other -> assertFailure ("expected HandshakeFailed, got " <> show other)
    Right _ -> assertFailure "no error, expected HandshakeFailed"

chosen :: [TestTree]
chosen =
  [ testCase "the opening request" $ do
      (request, ()) <- withRawServer (accepting []) (\settings -> withConnection settings (\_ -> pure ()))
      let requestLines = BS8.lines (BS8.filter (/= '\r') request)
      take 1 requestLines @?= ["GET / HTTP/1.1"]
      filter (`elem` requestLines) ["Upgrade: websocket", "Connection: Upgrade", "Sec-WebSocket-Version: 13"]
        @?= ["Upgrade: websocket", "Connection: Upgrade", "Sec-WebSocket-Version: 13"]
  , testCase "the accept value of RFC 6455, section 1.3" $
      acceptFor "dGhlIHNhbXBsZSBub25jZQ==" @?= "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
  , testCase "a fragmented message is put together, pings in the middle answered" $ do
      let sent =
            [ Frame False TextFrame "Hel"
            , Frame True PingFrame "mid"
            , Frame False Continuation "lo, "
            , Frame True Continuation "world"
            , Frame True BinaryFrame "\1\2"
            ]
      (_, received) <- withRawServer (accepting sent) $ \settings ->
        withConnection settings (Stream.toList . messages)
      received @?= [TextMessage "Hello, world", BinaryMessage "\1\2"]
  , testCase "a server that goes without closing ends the messages" $ do
      (_, received) <- withRawServer (accepting [Frame True TextFrame "only"]) $ \settings ->
        withConnection settings (Stream.toList . messages)
      received @?= [TextMessage "only"]
  , testCase "the server refuses: another status, a wrong proof, no upgrade, an extension, an endless answer" $ do
      refused (const ["HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"])
      refused (const ["HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"])
      refused (\key -> ["HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " <> acceptFor key <> "\r\n\r\n"])
      refused (\key -> ["HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: " <> acceptFor key <> "\r\n\r\n"])
      refused (\key -> ["HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Extensions: permessage-deflate\r\nSec-WebSocket-Accept: " <> acceptFor key <> "\r\n\r\n"])
      refused (const ["SSH-2.0-OpenSSH\r\n"])
      refused (\key -> ["HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" <> BS.concat (replicate 2000 "X-Padding: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n") <> "Sec-WebSocket-Accept: " <> acceptFor key <> "\r\n\r\n"])
  , testCase "what breaks the protocol is reported, and what came before delivered" $ do
      let breaking sent = do
            (_, outcome) <- withRawServer (accepting (Frame True TextFrame "fine" : sent)) $ \settings ->
              withConnection settings $ \connection -> do
                first <- receive connection
                second <- try (receive connection)
                third <- receive connection
                pure (first, second, third)
            case outcome of
              (Just (TextMessage "fine"), Left (ProtocolViolation _), Nothing) -> pure ()
              other -> assertFailure ("unexpected: " <> show other)
      breaking [Frame True Continuation "nothing to continue"]
      breaking [Frame False TextFrame "a", Frame True TextFrame "b"]
      breaking [Frame True TextFrame "\255\254"]
      breaking [Frame True CloseFrame "\3"]
  , testCase "a message beyond the limit, in one frame or in several" $ do
      let tooLarge sent = do
            (_, outcome) <- withRawServer (accepting sent) $ \settings ->
              try (withConnection settings {maxMessageSize = 10} receive)
            outcome @?= Left (MessageTooLarge 10)
      tooLarge [Frame True BinaryFrame "12345678901"]
      tooLarge [Frame False BinaryFrame "123456", Frame True Continuation "78901"]
  , testCase "a server that accepts and never answers the upgrade" $
      bracket listening Net.close $ \listener -> do
        port <- Net.socketPort listener
        let hold = bracket (fst <$> Net.accept listener) Net.close (\_ -> threadDelay 10_000_000)
        withAsync hold $ \_ ->
          try (withConnection (config "127.0.0.1" (fromIntegral port) "/") {openTimeout = Just 150_000} (\_ -> pure ())) >>= (@?= Left OpenTimedOut)
  , testCase "a path that cannot be written is refused before any connection" $
      try (withConnection (config "127.0.0.1" 1 "/a b") (\_ -> pure ())) >>= \case
        Left (InvalidRequest _) -> pure ()
        Left other -> assertFailure ("expected InvalidRequest, got " <> show other)
        Right () -> assertFailure "no error, expected InvalidRequest"
  , testCase "nothing listening is an IOError" $ do
      port <- bracket listening Net.close Net.socketPort
      try (withConnection (config "127.0.0.1" (fromIntegral port) "/") (\_ -> pure ())) >>= \case
        Left (_ :: IOError) -> pure ()
        Right () -> assertFailure "no error, expected an IOError"
  ]

-- | Serve with a handler on a free port of the loopback interface, and
-- run something against the port.
withOwnServer :: (Server.Upgrade -> Connection -> IO ()) -> (Int -> IO a) -> IO a
withOwnServer handler run =
  bracket (Server.listen "127.0.0.1" 0) Net.close $ \listener -> do
    port <- Net.socketPort listener
    withAsync (Server.serveSocket listener Server.defaultServerOptions handler) $ \_ -> run (fromIntegral port)

-- | Send back every message received.
echoBack :: Server.Upgrade -> Connection -> IO ()
echoBack _ connection = Stream.fold (Fold.drainMapM (send connection)) (messages connection)

serving :: [TestTree]
serving =
  [ testCase "messages of a websockets client come back, of every length form" $ do
      let sizes = [0, 125, 126, 65535, 65536, 300_000]
      received <- withOwnServer echoBack $ \port ->
        WS.runClient "127.0.0.1" port "/" $ \connection -> do
          WS.sendTextData connection ("héllo" :: Text.Text)
          text <- WS.receiveData connection
          sizesBack <- mapM (\size -> WS.sendBinaryData connection (BS.replicate size 7) >> BS.length <$> WS.receiveData connection) sizes
          WS.sendClose connection ("" :: ByteString)
          pure (text :: Text.Text, sizesBack)
      received @?= ("héllo", sizes)
  , testCase "the handler sees the path and the headers asked" $ do
      asked <- newEmptyMVar
      withOwnServer (\upgrade _ -> putMVar asked upgrade) $ \port ->
        WS.runClientWith "127.0.0.1" port "/chat?room=1" WS.defaultConnectionOptions [("X-Extra", "yes")] (\_ -> pure ())
      upgrade <- takeMVar asked
      (upgrade.path, lookup "x-extra" upgrade.headers, lookup "host" upgrade.headers) @?= ("/chat?room=1", Just "yes", lookup "host" upgrade.headers)
      fmap (BS.isPrefixOf "127.0.0.1") (lookup "host" upgrade.headers) @?= Just True
  , testCase "a ping of the client is answered while the server receives" $ do
      answer <- withOwnServer echoBack $ \port ->
        WS.runClient "127.0.0.1" port "/" $ \connection -> do
          WS.sendPing connection ("are you there" :: ByteString)
          WS.receive connection
      answer @?= WS.ControlMessage (WS.Pong "are you there")
  , testCase "a handler that returns closes the connection in order" $ do
      outcome <- withOwnServer (\_ connection -> send connection (TextMessage "bye")) $ \port ->
        WS.runClient "127.0.0.1" port "/" $ \connection -> do
          first <- WS.receiveData connection
          closed <- try (WS.receiveDataMessage connection)
          pure (first :: Text.Text, either (\case WS.CloseRequest status _ -> Just status; _ -> Nothing) (const Nothing) closed)
      outcome @?= ("bye", Just 1000)
  , testCase "a client that closes ends the messages of the handler" $ do
      seen <- newEmptyMVar
      withOwnServer (\_ connection -> Stream.toList (messages connection) >>= putMVar seen) $ \port ->
        WS.runClient "127.0.0.1" port "/" $ \connection -> do
          WS.sendTextData connection ("one" :: Text.Text)
          WS.sendTextData connection ("two" :: Text.Text)
          WS.sendClose connection ("" :: ByteString)
          void (try @WS.ConnectionException (forever (WS.receiveDataMessage connection)))
      takeMVar seen >>= (@?= [TextMessage "one", TextMessage "two"])
  , testCase "several clients are served at once" $ do
      answers <- withOwnServer echoBack $ \port ->
        mapConcurrently
          ( \index -> WS.runClient "127.0.0.1" port "/" $ \connection -> do
              WS.sendTextData connection (Text.pack (show index))
              answer <- WS.receiveData connection
              WS.sendClose connection ("" :: ByteString)
              pure (answer :: Text.Text)
          )
          [1 .. 20 :: Int]
      answers @?= map (Text.pack . show) [1 .. 20 :: Int]
  ]

serverSide :: [TestTree]
serverSide =
  [ testCase "the client and the server of this library talk to each other" $ do
      outcome <- withOwnServer echoBack $ \port ->
        withConnection (config "127.0.0.1" port "/") $ \connection -> do
          -- The ping first: its pong then comes before the messages, and is
          -- counted while they are received.
          ping connection "x"
          send connection (TextMessage "héllo")
          send connection (BinaryMessage (BS.replicate 70_000 9))
          answers <- Stream.toList (Stream.take 2 (messages connection))
          pongs <- atomically (pongCount connection)
          pure (answers, pongs)
      outcome @?= ([TextMessage "héllo", BinaryMessage (BS.replicate 70_000 9)], 1)
  , testCase "a ping of the server is answered by the client, and counted" $ do
      counted <- newEmptyMVar
      let handler _ connection = do
            ping connection ""
            -- The pong is counted while something receives.
            withAsync (Stream.toList (messages connection)) $ \_ ->
              atomically (pongCount connection >>= check . (> 0))
            atomically (pongCount connection) >>= putMVar counted
      received <- withOwnServer handler $ \port ->
        withConnection (config "127.0.0.1" port "/") (Stream.toList . messages)
      takeMVar counted >>= (@?= 1)
      received @?= []
  , testCase "what does not ask for a WebSocket is answered 400, and the server goes on" $ do
      (answers, served) <- withOwnServer echoBack $ \port -> do
        let talk request = bracket (connect "127.0.0.1" port) Net.close $ \socket -> do
              NetBS.sendAll socket request
              NetBS.recv socket 4096
        plain <- talk "GET / HTTP/1.1\r\nHost: x\r\n\r\n"
        notHttp <- talk "SSH-2.0-OpenSSH\r\n\r\n"
        oldVersion <- talk "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 8\r\n\r\n"
        after <- withConnection (config "127.0.0.1" port "/") (\connection -> send connection (TextMessage "still serving") >> receive connection)
        pure (map (BS.takeWhile (/= 13)) [plain, notHttp, oldVersion], after)
      answers @?= replicate 3 "HTTP/1.1 400 Bad Request"
      served @?= Just (TextMessage "still serving")
  , testCase "the answer to a proper request, and a frame that is not masked" $ do
      outcome <- newEmptyMVar
      answer <- withOwnServer (\_ connection -> try (receive connection) >>= putMVar outcome) $ \port ->
        bracket (connect "127.0.0.1" port) Net.close $ \socket -> do
          NetBS.sendAll socket "GET /x HTTP/1.1\r\nHost: x\r\nUpgrade: WebSocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
          answer <- NetBS.recv socket 4096
          NetBS.sendAll socket (encodeFrame Nothing (Frame True TextFrame "not masked"))
          takeMVar outcome >>= \case
            Left (ProtocolViolation _) -> pure ()
            other -> assertFailure ("expected ProtocolViolation, got " <> show other)
          pure answer
      answer @?= "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
  , testCase "a client that never sends its request" $
      bracket (Server.listen "127.0.0.1" 0) Net.close $ \listener -> do
        port <- Net.socketPort listener
        let silent = bracket (connect "127.0.0.1" (fromIntegral port)) Net.close (\_ -> threadDelay 10_000_000)
        withAsync silent $ \_ ->
          bracket (fst <$> Net.accept listener) Net.close $ \peer ->
            try (Server.acceptOver (socketTransport peer) Server.defaultServerOptions {Server.openTimeout = Just 150_000} (\_ _ -> pure ())) >>= (@?= Left OpenTimedOut)
  ]
