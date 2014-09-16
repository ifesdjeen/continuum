{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveGeneric #-}

module Continuum.Cluster where

import           GHC.Generics           (Generic)
import Data.Serialize
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM

import qualified Control.Concurrent.Suspend.Lifted as Delay
import qualified Control.Concurrent.Timer as Timer

import           Control.Monad.State.Strict
import qualified Nanomsg as N
import qualified Data.ByteString.Char8 as C
import qualified Network.Socket as Socket

import           Control.Monad.IO.Class (liftIO)
import           Control.Monad (replicateM_)

nLat :: Int -> Int -> String -> String -> IO ()
nLat size count bindString connString = do
      s1 <- N.socket N.Pair
      _ <- N.bind s1 bindString
      s2 <- N.socket N.Pair
      _ <- N.connect s2 connString
      -- let msg = C.pack $ replicate size 'a'
      -- replicateM_ count (N.send s1 msg >> N.recv s2 >>= N.send s2 >> (liftIO . print) $ N.recv s1)
      N.send s1 (C.pack $ replicate size 'a')
      N.send s1 (C.pack $ replicate size 'b')
      N.send s1 (C.pack $ replicate size 'c')

      received <- N.recv s2
      liftIO $ print received
      received <- N.recv s2
      liftIO $ print received
      received <- N.recv s2
      liftIO $ print received

      N.send s2 (C.pack $ replicate size 'a')
      N.send s2 (C.pack $ replicate size 'b')
      N.send s2 (C.pack $ replicate size 'c')

      received <- N.recv s1
      liftIO $ print received
      received <- N.recv s1
      liftIO $ print received
      received <- N.recv s1
      liftIO $ print received

      N.close s1
      N.close s2
      return ()

data Node = Node String Int
          deriving(Generic, Show)

instance Serialize Node

data NodeContext = NodeContext { tcpServer       :: N.Socket N.Pair,
                                 tcpClients      :: [N.Socket N.Pair]

                               }

type NodeState a = StateT NodeContext IO a

startSeedNode = do
  done <- newEmptyMVar

  shared <- atomically $ newTVar ([] :: [Node])

  forkIO $ do
    serverSocket <- N.socket N.Pair
    _ <- N.bind serverSocket "tcp://*:5566"
    let receiveop = do
          received <- N.recv serverSocket
          case (decode received :: Either String Node) of
            (Left _) -> putMVar done ()
            (Right a) -> appV (\i -> (i ++ [a])) shared
          receiveop
    receiveop

  forkIO $ do
    Timer.repeatedTimer (dispVar shared) (Delay.msDelay 5000)
    return ()

  takeMVar done

  return ()


-- Server Socket is used to recevie messages from all the nodes
-- Server socket may be also used to broadcase messages to all the nodes
-- Client socket is udes to push responses back messages from any other node (one at a time)

startNormalNode = do

  shared <- atomically $ newTVar ([] :: [Node])

  -- forkIO $ do
  --   serverSocket <- N.socket N.Pair
  --   _ <- N.bind serverSocket "tcp://*:5577"
  --   received <- N.recv serverSocket
  --   print $ (decode received :: Either String Node)

  forkIO $ do
    clientSocket <- N.socket N.Pair
    _ <- N.bind clientSocket "tcp://*:5567"
    _ <- N.connect clientSocket "tcp://127.0.0.1:5566"
    N.send clientSocket (encode $ Node "123" 555)
    N.close clientSocket
  return ()


-- main =
--   nLat 40 1000 "tcp://*:5566" "tcp://127.0.0.1:5566"

-- startNode = Socket.withSocketsDo $ do
--   bcClient <- Multicast.multicastReceiver "0.0.0.0" 4444

--   let loop = do
--         (msg, _, addr) <- Socket.recvFrom bcClient 1024
--         print (msg, addr)
--   timer1 <- Timer.repeatedTimer (loop) (Delay.msDelay 5000)

--   (bcServer, bcAddr) <- Multicast.multicastSender "127.0.0.1" 4444
--   timer <- Timer.repeatedTimer (Socket.sendTo bcServer "hier bin ich" bcAddr >> return ()) (Delay.msDelay 5000)
--   return ()


-- main =
--   nLat 40 1000 "tcp://*:5566" "tcp://127.0.0.1:5566"

atomRead = atomically . readTVar
dispVar x = atomRead x >>= print
appV fn x = atomically $ readTVar x >>= writeTVar x . fn
