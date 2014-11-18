{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE RecordWildCards #-}

module Continuum.Cluster where

import           Continuum.Types
import           Continuum.Common.Types
import           Continuum.Storage
import           Continuum.Folds

import           System.Process                 ( system )
import           Control.Concurrent             ( forkIO, newEmptyMVar, putMVar, takeMVar, MVar )
import           Control.Concurrent.STM
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Monad

import           Control.Exception.Base         ( bracket )
import           Control.Monad.Trans.Resource   ( runResourceT )
import           Control.Monad.State.Strict     ( runStateT )
import           Data.Serialize                 ( encode, decode )
-- import           Continuum.Internal.Directory   ( mkdir )
import           Control.Applicative            ( (<$>) )

import qualified Database.LevelDB.Base               as LDB
import qualified Continuum.Options                   as Opts
import qualified Data.Map                            as Map
import qualified Data.Time.Clock.POSIX               as Clock
import qualified Control.Concurrent.Suspend.Lifted   as Delay
import qualified Control.Concurrent.Timer            as Timer

import qualified Nanomsg as N

import           Debug.Trace                    ( trace )

type Socket = N.Socket N.Rep

-- |
-- | Reuqest Processing
-- |

processRequest :: Socket
                  -> TVar DBContext
                  -> Request
                  -> IO ()

processRequest socket shared (RunQuery query) = do
  resp  <- runQuery shared query
  _     <- N.send socket (encode $ resp)
  return ()

-- |
-- | Query
-- |


runQuery :: TVar DBContext -> Query -> IO (DbErrorMonad DbResult)
runQuery shared (CreateDb name schema) = do
  ctx <- atomRead shared
  (res, newst) <- runAppState ctx (createDatabase name schema)
  atomReset newst shared
  return res

runQuery shared (Insert name record) = do
  ctx <- atomRead shared
  (res, newst) <- runAppState ctx (putRecord name record)
  atomReset newst shared
  return res

runQuery shared (FetchAll name) = do
  ctx <- atomRead shared
  (res, newst) <- runAppState ctx (scan name EntireKeyspace Record appendFold)
  atomReset newst shared
  return (DbResults <$> res)

runQuery _ other = do
  _ <- print $ other
  return (Right EmptyRes)

runAppState :: DBContext
               -> AppState a
               -> IO (DbErrorMonad a,
                      DBContext)
runAppState = flip runStateT


startStorage :: String -> IO (TVar DBContext)
startStorage path = do
  _           <- system ("mkdir " ++ path)
  systemDb    <- LDB.open (path ++ "/system") Opts.opts
  chunksDb    <- LDB.open (path ++ "/chunksDb") Opts.opts

  (Right dbs) <- initializeDbs path systemDb

  let context = DBContext {ctxPath           = path,
                           ctxSystemDb       = systemDb,
                           ctxDbs            = dbs,
                           ctxChunksDb       = chunksDb,
                           sequenceNumber    = 1,
                           lastSnapshot      = 1,
                           ctxRwOptions      = (Opts.readOpts,
                                                Opts.writeOpts)}

  shared <- atomically $ newTVar context

  return shared

stopStorage :: TVar DBContext -> IO ()
stopStorage shared = do
  DBContext{..} <- atomRead shared
  _             <- LDB.close ctxSystemDb
  _             <- LDB.close ctxChunksDb
  _             <- mapM (\(_, (_, db)) -> LDB.close db) (Map.toList ctxDbs)
  return ()

withStorage :: String
               -> (TVar DBContext -> IO a)
               -> IO a

withStorage path subsystem = do
  bracket (startStorage path)
          stopStorage
          subsystem

withTmpStorage :: String
               -> IO ()
               -> (TVar DBContext -> IO a)
               -> IO a
withTmpStorage path cleanup subsystem =
  bracket (startStorage path)
          (\i -> stopStorage i >> cleanup)
          subsystem


startClientAcceptor :: MVar ()
                       -> String
                       -> TVar DBContext
                       -> IO (N.Socket N.Rep, N.Endpoint)
startClientAcceptor startedMVar port shared = do
  serverSocket <- N.socket N.Rep
  endpoint     <- N.bind serverSocket ("tcp://*:" ++ port)

  _            <- putMVar startedMVar ()

  return (serverSocket, endpoint)

stopClientAcceptor :: MVar () -> (N.Socket N.Rep, N.Endpoint) -> IO ()
stopClientAcceptor doneMVar (serverSocket, endpoint) = do
  _    <- N.shutdown serverSocket endpoint
  _    <- N.close serverSocket

  _    <- putMVar doneMVar ()

  return ()

withClientAcceptor :: MVar ()
                      -> MVar ()
                      -> String
                      -> TVar DBContext
                      -> IO ()

withClientAcceptor startedMVar doneMVar port shared =
  bracket (startClientAcceptor startedMVar port shared)
          (stopClientAcceptor doneMVar)
          (\(serverSocket, _) -> receiveLoop serverSocket shared)

startNode :: MVar ()
             -> MVar ()
             -> String -> String
             -> IO ()
startNode startedMVar doneMVar path port = do
  withStorage path $
    withClientAcceptor startedMVar doneMVar port


emptyResult :: DbErrorMonad DbResult
emptyResult = Right EmptyRes

receiveLoop :: N.Socket N.Rep -> TVar DBContext -> IO ()
receiveLoop serverSocket shared = do
  received <- N.recv serverSocket

  case (decode received :: Either String Request) of
    (Right Shutdown) -> do
      print "Received Shutdown"
      N.send serverSocket (encode emptyResult)
      return ()

    (Left err)       -> do
      print ("Can't decode message: " ++ (show err))
      receiveLoop serverSocket shared

    (Right request)  -> do
      print ("Request received: " ++ (show request))
      processRequest serverSocket shared request
      receiveLoop serverSocket shared

-- Server Socket is used to recevie messages from all the nodes
-- Server socket may be also used to broadcase messages to all the nodes
-- Client socket is udes to push responses back messages from any other node (one at a time)

atomRead :: TVar a -> IO a
atomRead = atomically . readTVar

swap :: (b -> b) -> TVar b -> IO ()
swap fn x = atomically $ readTVar x >>= writeTVar x . fn

atomReset :: b -> TVar b -> IO ()
atomReset newv x = atomically $ writeTVar x newv

nodeTimeout :: Clock.POSIXTime
nodeTimeout = 5

insertNode :: Clock.POSIXTime -> Node -> ClusterNodes -> ClusterNodes
insertNode time node = Map.insert node (NodeStatus time)
