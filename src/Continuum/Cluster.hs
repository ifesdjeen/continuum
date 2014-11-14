{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}

module Continuum.Cluster where

import           Continuum.Types
import           Continuum.Storage

import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Monad.IO.Class
import           Control.Monad

import           System.Environment

import           Control.Monad.Trans.Resource   ( runResourceT )
import           Data.Serialize                 ( encode, decode )
import           Continuum.Internal.Directory   ( mkdir )
import           Control.Applicative            ( (<$>) )

import qualified Database.LevelDB.MonadResource      as LDB
import qualified Continuum.Options                   as Opts
import qualified Data.Map                            as Map
import qualified Data.Time.Clock.POSIX               as Clock
import qualified Control.Concurrent.Suspend.Lifted   as Delay
import qualified Control.Concurrent.Timer            as Timer

import qualified Nanomsg as N

type Socket = N.Socket N.Bus

-- |
-- | Reuqest Processing
-- |

processRequest :: Socket
                  -> TVar DBContext
                  -> Request
                  -> IO ()
processRequest socket shared (ImUp node) = do
  nodes <- ctxNodes <$> atomRead shared
  _     <- N.send socket (encode $ NodeList (Map.keys nodes))
  time  <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time node) shared
  return ()

processRequest socket shared (ImUp node) = do
  nodes <- ctxNodes <$> atomRead shared
  _     <- N.send socket (encode $ NodeList (Map.keys nodes))
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

processRequest socket shared (NodeList nodeList) = do
  self <- ctxSelfNode <$> atomRead shared
  forM_ nodeList (connectTo socket self shared >> return)
  return ()

-- Migrate processrequest to its own typeclass
processRequest socket shared (Query query) = do
  resp <- runQuery shared query
  _     <- N.send socket (encode $ resp)
  return ()

runQuery :: TVar DBContext -> Query -> IO (DbErrorMonad DbResult)
runQuery shared (CreateDb name schema) = do
  ctx <- atomRead shared
  (res, newst) <- runAppState ctx (createDatabase name schema)
  reset newst shared
  return res

connectTo :: N.Socket N.Bus
             -> Node
             -> TVar DBContext
             -> Node
             -> IO ()
connectTo socket self shared other@(Node host port) | other /= self = do
  time <- Clock.getPOSIXTime
  _    <- swap (fmapNodes $ insertNode time other) shared
  _    <- N.connect socket ("tcp://" ++ host ++ ":" ++ port)
  _    <- Timer.repeatedTimer introduce (Delay.msDelay 1000)
  return ()
  where introduce = N.send socket (encode $ Introduction self)
connectTo _ _ _ _ = return ()

initializeNode :: N.Socket N.Bus
                  -> Node
                  -> Node
                  -> TVar DBContext
                  -> IO ()
initializeNode socket seed@(Node seedHost seedPort) self@(Node _ port) shared= do
  _ <- N.bind socket ("tcp://*:" ++ port)
  when (seedHost == "127.0.0.1") initialize
  where initialize = do
          _    <- N.connect socket ("tcp://" ++ seedHost ++ ":" ++ seedPort)
          time <- Clock.getPOSIXTime
          _    <- swap (fmapNodes $ insertNode time seed) shared
          _    <- N.send socket (encode $ ImUp self)
          return ()

-- startNode2 :: (LDB.MonadResource m) => m ()
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

  serverSocket <- liftIO $ N.socket N.Bus

  let seed         = (Node seedHost seedPort)
      self         = (Node host port)
      boundRequest = processRequest serverSocket

  liftIO $ initializeNode serverSocket seed self shared

  -- |
  -- | Receive Loop
  -- |

  _ <- liftIO . forkIO $ do
    let receiveop = do
          received <- N.recv serverSocket
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
    _ <- Timer.repeatedTimer (sendHeartbeat serverSocket self) (Delay.msDelay 1000)
    return ()

  liftIO $ takeMVar done

  return ()

sendHeartbeat :: N.Socket N.Bus -> Node -> IO ()
sendHeartbeat socket node = N.send socket (encode $ Heartbeat node)

-- Server Socket is used to recevie messages from all the nodes
-- Server socket may be also used to broadcase messages to all the nodes
-- Client socket is udes to push responses back messages from any other node (one at a time)

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
