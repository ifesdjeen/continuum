{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveGeneric #-}

module Continuum.Cluster where

import qualified Continuum.Storage as Storage
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Monad
import           Data.Serialize
import qualified Data.Map as Map
import qualified Data.Time.Clock.POSIX as Clock
import           GHC.Generics           (Generic)
import           System.Environment

import qualified Control.Concurrent.Suspend.Lifted as Delay
import qualified Control.Concurrent.Timer as Timer

import qualified Nanomsg as N
import qualified Data.ByteString.Char8 as C
import qualified Network.Socket as Socket

import           Control.Monad.IO.Class (liftIO)
import           Control.Monad (replicateM_)

data Node = Node String String
          deriving(Generic, Show, Eq, Ord)

data NodeStatus = NodeStatus {lastHeartbeat :: Clock.POSIXTime}
                deriving(Generic, Eq, Ord)

type ClusterNodes = Map.Map Node NodeStatus

instance Serialize Node

processRequest :: Node
                  -> N.Socket N.Bus
                  -> TVar ClusterNodes
                  -> Request
                  -> IO ()
processRequest self socket shared (ImUp node) = do
  nodes <- atomRead shared
  N.send socket (encode $ NodeList (Map.keys nodes))
  time <- Clock.getPOSIXTime
  swap (\nodes -> Map.insert node (NodeStatus time) nodes) shared
  return ()

processRequest self socket shared (Heartbeat node) = do
  time <- Clock.getPOSIXTime
  swap (\nodes -> Map.update (updateNodeTime time) node nodes) shared
  where updateNodeTime time nodeStatus = Just $ nodeStatus { lastHeartbeat = time }

processRequest self socket shared (Introduction node) = do
  time <- Clock.getPOSIXTime
  swap (\nodes -> Map.insert node (NodeStatus time) nodes) shared
  return ()

processRequest self socket nodes (NodeList nodeList) = do
  forM nodeList connectToCluster
  return ()
  where connectToCluster node = do
          connectTo socket node self nodes
          return ()

connectTo :: N.Socket N.Bus
             -> Node
             -> Node
             -> TVar ClusterNodes
             -> IO ()
connectTo socket other@(Node host port) self shared | other /= self = do
  time <- Clock.getPOSIXTime
  swap (\nodes -> Map.insert other (NodeStatus time) nodes) shared
  N.connect socket ("tcp://" ++ host ++ ":" ++ port)
  Timer.repeatedTimer introduce (Delay.msDelay 1000)
  return ()
  where introduce = N.send socket (encode $ Introduction self)

connectTo socket other@(Node host port) self shared = return ()
initializeNode :: N.Socket N.Bus
                  -> Node
                  -> Node
                  -> TVar ClusterNodes
                  -> IO ()
initializeNode socket seed@(Node seedHost seedPort) self@(Node _ port) shared= do
  _ <- N.bind socket ("tcp://*:" ++ port)
  when (seedHost == "127.0.0.1") init
  where init = do
          N.connect socket ("tcp://" ++ seedHost ++ ":" ++ seedPort)
          time <- Clock.getPOSIXTime
          swap (\nodes -> Map.insert seed (NodeStatus time) nodes) shared
          N.send socket (encode $ ImUp self)
          return ()

startNode = do
  done <- newEmptyMVar

  [host, port, seedHost, seedPort] <- getArgs
  shared <- atomically $ newTVar (Map.empty :: ClusterNodes)

  serverSocket <- N.socket N.Bus

  let seed         = (Node seedHost seedPort)
      self         = (Node host port)
      boundRequest = processRequest self serverSocket

  initializeNode serverSocket seed self shared

  forkIO $ do
    let receiveop = do
          received <- N.recv serverSocket
          case (decode received :: Either String Request) of
            (Left _)  -> putMVar done ()
            (Right request) -> boundRequest shared request
          receiveop
    receiveop

  forkIO $ do
    Timer.repeatedTimer (printClusterStatus shared) (Delay.msDelay 1000)
    Timer.repeatedTimer (sendHeartbeat serverSocket self) (Delay.msDelay 1000)
    return ()

  takeMVar done

  return ()

sendHeartbeat :: N.Socket N.Bus -> Node -> IO ()
sendHeartbeat socket node = N.send socket (encode $ Heartbeat node)

-- Server Socket is used to recevie messages from all the nodes
-- Server socket may be also used to broadcase messages to all the nodes
-- Client socket is udes to push responses back messages from any other node (one at a time)

atomRead = atomically . readTVar
dispVar x = atomRead x >>= print

printClusterStatus :: TVar ClusterNodes -> IO ()
printClusterStatus cluster = do
  val <- atomRead cluster
  time <- Clock.getPOSIXTime
  print $ map (\(k,v) -> (k, showNodeStatus v time)) (Map.toList val)
  print "------------"

showNodeStatus :: NodeStatus -> Clock.POSIXTime -> String
showNodeStatus nst currentTime =
  if (lastHeartbeat nst) >= (currentTime - nodeTimeout)
  then "active"
  else "down"

swap fn x = atomically $ readTVar x >>= writeTVar x . fn

--- Protocol Specofication

data Request = ImUp Node
             | Introduction Node
             | NodeList [Node]
             | Heartbeat Node
             deriving(Generic, Show)

instance Serialize Request

nodeTimeout :: Clock.POSIXTime
nodeTimeout = 5
