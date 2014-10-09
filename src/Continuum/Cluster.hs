{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveGeneric #-}

module Continuum.Cluster where

import           Control.Applicative ((<$>))
import           Data.ByteString (ByteString)
import qualified Database.LevelDB.MonadResource as LDB
import           Control.Monad.Trans.Resource (runResourceT)
import           Continuum.Types
import           Continuum.Storage
import qualified Continuum.Options as Opts
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Monad.IO.Class
import           Control.Monad
import           Data.Serialize (encode, decode)
import qualified Data.Map as Map
import qualified Data.Time.Clock.POSIX as Clock
import           GHC.Generics           (Generic)
import           System.Environment

import qualified Control.Concurrent.Suspend.Lifted as Delay
import qualified Control.Concurrent.Timer as Timer

import qualified System.ZMQ4 as Zmq


data Sockets = Sockets  { pubSocket :: Zmq.Socket Zmq.Dealer,
                          subSocket :: Zmq.Socket Zmq.Router }

processRequest :: Sockets
                  -> TVar DBContext
                  -> Request
                  -> IO ()
processRequest sockets shared (ImUp node) = do
  nodes <- ctxNodes <$> atomRead shared
  _     <- send sockets (encode $ NodeList (Map.keys nodes))
  time  <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time node) shared
  return ()

processRequest sockets shared (ImUp node) = do
  nodes <- ctxNodes <$> atomRead shared
  _     <- send sockets (encode $ NodeList (Map.keys nodes))
  time  <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time node) shared
  return ()

processRequest _ shared (Heartbeat node) = do
  time  <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time node) shared
  return ()
  where updateNodeTime time nodeStatus = Just $ nodeStatus { lastHeartbeat = time }

processRequest _ shared (Introduction node) = do
  time <- Clock.getPOSIXTime
  _    <- swap (fmapNodes $ insertNode time node) shared
  return ()

processRequest sockets shared (NodeList nodeList) = do
  self <- ctxSelfNode <$> atomRead shared
  forM_ nodeList (connectTo sockets self shared >> return)
  return ()

-- Migrate processrequest to its own typeclass
processRequest sockets shared (Query query) = do
  resp <- runQuery shared query
  _     <- send sockets (encode $ resp)
  return ()

runQuery :: TVar DBContext -> Query -> IO (DbErrorMonad DbResult)
runQuery shared (CreateDb name schema) = do
  ctx <- atomRead shared
  (res, newst) <- runAppState ctx (createDatabase name schema)
  reset newst shared
  return res

connectTo :: Sockets
             -> Node
             -> TVar DBContext
             -> Node
             -> IO ()
connectTo sockets self shared other@(Node host port) | other /= self = do
  time <- Clock.getPOSIXTime
  _    <- swap (fmapNodes $ insertNode time other) shared
  _    <- Zmq.connect (subSocket sockets) ("tcp://" ++ host ++ ":" ++ port)
  _    <- Timer.repeatedTimer introduce (Delay.msDelay 1000)
  return ()
  where introduce = send sockets (encode $ Introduction self)

connectTo _ _ _ _ = return ()

initializeNode :: Sockets
                  -> Node
                  -> Node
                  -> TVar DBContext
                  -> IO ()
initializeNode sockets seed@(Node seedHost seedPort) self@(Node _ port) shared= do
  _ <- Zmq.bind (pubSocket sockets) ("tcp://*:" ++ port)
  when (seedHost == "127.0.0.1") initialize
  where initialize = do
          _    <- Zmq.connect (subSocket sockets) ("tcp://" ++ seedHost ++ ":" ++ seedPort)
          time <- Clock.getPOSIXTime
          _    <- swap (fmapNodes $ insertNode time seed) shared
          _    <- send sockets (encode $ ImUp self)
          return ()

-- -- startNode2 :: (LDB.MonadResource m) => m ()
startNode :: IO ()
startNode = runResourceT $ do
  done <- liftIO newEmptyMVar

  [path, host, port, seedHost, seedPort] <- liftIO getArgs

  systemDb    <- LDB.open (path ++ "/system") Opts.opts
  chunksDb    <- LDB.open (path ++ "/chunksDb") Opts.opts
  (Right dbs) <- initializeDbs path systemDb

  let context = DBContext {ctxPath           = path,
                           ctxSystemDb       = systemDb,
                           ctxNodes          = Map.empty,
                           ctxSelfNode       = Node host port,
                           ctxDbs            = dbs,
                           ctxChunksDb       = chunksDb,
                           sequenceNumber    = 1,
                           lastSnapshot      = 1,
                           ctxRwOptions      = (Opts.readOpts,
                                                Opts.writeOpts)}

  shared <- liftIO $ atomically $ newTVar context

  zmqContext <- liftIO $ Zmq.context
  pubSocket' <- liftIO $ Zmq.socket zmqContext Zmq.Dealer
  subSocket' <- liftIO $ Zmq.socket zmqContext Zmq.Router

  let sockets      = Sockets {pubSocket = pubSocket', subSocket = subSocket'}
      seed         = (Node seedHost seedPort)
      self         = (Node host port)
      boundRequest = processRequest sockets

  liftIO $ initializeNode sockets seed self shared

  -- |
  -- | Receive Loop
  -- |

  _ <- liftIO . forkIO $ do
    let receiveop = do
          received <- receive sockets
          case (decode received :: Either String Request) of
            (Left _)  -> putMVar done ()
            (Right request) -> boundRequest shared request
          receiveop
    receiveop

  -- |
  -- | Hearbeat loop
  -- |

  _ <- liftIO . forkIO $ do
    _ <- Timer.repeatedTimer (printClusterStatus shared) (Delay.msDelay 1000)
    _ <- Timer.repeatedTimer (sendHeartbeat sockets self) (Delay.msDelay 1000)
    return ()

  liftIO $ takeMVar done

  return ()

sendHeartbeat :: Sockets -> Node -> IO ()
sendHeartbeat sockets node = send sockets (encode $ Heartbeat node)

-- -- Server Socket is used to recevie messages from all the nodes
-- -- Server socket may be also used to broadcase messages to all the nodes
-- -- Client socket is udes to push responses back messages from any other node (one at a time)

atomRead :: TVar a -> IO a
atomRead = atomically . readTVar

printClusterStatus :: TVar DBContext -> IO ()
printClusterStatus shared = do
  val  <- ctxNodes <$> atomRead shared
  time <- Clock.getPOSIXTime
  print $ map (\(k,v) -> (k, showNodeStatus v time)) (Map.toList val)
  print ("------------" :: String)

showNodeStatus :: NodeStatus -> Clock.POSIXTime -> String
showNodeStatus nst currentTime =
  if (lastHeartbeat nst) >= (currentTime - nodeTimeout)
  then "active"
  else "down"

swap :: (b -> b) -> TVar b -> IO ()
swap fn x = atomically $ readTVar x >>= writeTVar x . fn

reset :: b -> TVar b -> IO ()
reset newv x = atomically $ writeTVar x newv

nodeTimeout :: Clock.POSIXTime
nodeTimeout = 5

insertNode :: Clock.POSIXTime -> Node -> ClusterNodes -> ClusterNodes
insertNode time node = Map.insert node (NodeStatus time)

send :: Sockets
        -> ByteString
        -> IO ()
send sockets msg = Zmq.send (pubSocket sockets) [] msg

receive :: Sockets
           -> IO ByteString
receive sockets = Zmq.receive (subSocket sockets)
