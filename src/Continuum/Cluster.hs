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

import qualified Nanomsg as N



processRequest :: Node
                  -> N.Socket N.Bus
                  -> TVar DBContext
                  -> Request
                  -> IO ()
processRequest _ socket shared (ImUp node) = do
  nodes <- ctxNodes <$> atomRead shared
  _     <- N.send socket (encode $ NodeList (Map.keys nodes))
  time  <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time node) shared
  return ()

processRequest _ socket shared (ImUp node) = do
  nodes <- ctxNodes <$> atomRead shared
  _     <- N.send socket (encode $ NodeList (Map.keys nodes))
  time  <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time node) shared
  return ()

processRequest _ _ shared (Heartbeat node) = do
  time  <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time node) shared
  return ()
  where updateNodeTime time nodeStatus = Just $ nodeStatus { lastHeartbeat = time }

processRequest _ _ shared (Introduction node) = do
  time <- Clock.getPOSIXTime
  _    <- swap (fmapNodes $ insertNode time node) shared
  return ()

processRequest self socket nodes (NodeList nodeList) = do
  forM_ nodeList connectToCluster
  return ()
  where connectToCluster node = do
          connectTo socket node self nodes
          return ()

-- Migrate processrequest to its own typeclass
-- processRequest _ socket shared val@(Query q) = do
--   nodes <- atomRead shared
--   _     <- N.send socket (encode $ NodeList (Map.keys nodes))
--   time  <- Clock.getPOSIXTime
--   _     <- swap (updateNodeList time) shared
--   return ()
--   where updateNodeList time nodes = Map.insert node (NodeStatus time) nodes

connectTo :: N.Socket N.Bus
             -> Node
             -> Node
             -> TVar DBContext
             -> IO ()
connectTo socket other@(Node host port) self shared | other /= self = do
  time <- Clock.getPOSIXTime
  _     <- swap (fmapNodes $ insertNode time other) shared
  _     <- N.connect socket ("tcp://" ++ host ++ ":" ++ port)
  _     <- Timer.repeatedTimer introduce (Delay.msDelay 1000)
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

runCommand :: TVar DBContext -> AppState a -> IO (Either DbError a)
runCommand shared op = do
  st <- atomRead shared
  runAppState st op


-- startNode2 :: (LDB.MonadResource m) => m ()
startNode :: IO ()
startNode = runResourceT $ do
  done <- liftIO newEmptyMVar

  [path, host, port, seedHost, seedPort] <- liftIO getArgs

  systemDb    <- LDB.open (path ++ "/system") Opts.opts
  chunksDb    <- LDB.open (path ++ "/chunksDb") Opts.opts
  (Right dbs) <- initializeDbs path systemDb

  let context = makeContext path systemDb dbs chunksDb (Opts.readOpts, Opts.writeOpts)

  shared <- liftIO $ atomically $ newTVar context

  serverSocket <- liftIO $ N.socket N.Bus

  let seed         = (Node seedHost seedPort)
      self         = (Node host port)
      boundRequest = processRequest self serverSocket

  liftIO $ initializeNode serverSocket seed self shared

  _ <- liftIO . forkIO $ do
    let receiveop = do
          received <- N.recv serverSocket
          case (decode received :: Either String Request) of
            (Left _)  -> putMVar done ()
            (Right request) -> boundRequest shared request
          receiveop
    receiveop

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

nodeTimeout :: Clock.POSIXTime
nodeTimeout = 5

insertNode :: Clock.POSIXTime -> Node -> ClusterNodes -> ClusterNodes
insertNode time node = Map.insert node (NodeStatus time)
