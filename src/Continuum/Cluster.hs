{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE RecordWildCards #-}

module Continuum.Cluster where

import           Continuum.Types
import           Continuum.Storage
import           Continuum.Folds


import           Control.Concurrent             ( putMVar, MVar )
import           Control.Concurrent.STM

import           Control.Exception.Base         ( bracket )
import           Data.Serialize                 ( encode, decode )
-- import           Continuum.Internal.Directory   ( mkdir )
import           Control.Applicative            ( (<$>) )

import qualified Nanomsg as N

-- import           Debug.Trace                    ( trace )

type Socket = N.Socket N.Rep

-- |
-- | Reuqest Processing
-- |

processRequest :: Socket
                  -> TVar DBContext
                  -> Request
                  -> IO ()
-- TODO: GET RID OF THAT
processRequest socket shared (Select dbName query) = do
  resp  <- runQuery shared dbName query
  _     <- N.send socket (encode $ resp)
  return ()

processRequest socket shared (CreateDb name schema) = do
  ctx          <- atomRead shared
  (res, newst) <- runAppState ctx (createDatabase name schema)
  _            <- atomReset newst shared
  _            <- N.send socket (encode $ res)
  return ()


processRequest socket shared (Insert name record) = do
  ctx          <- atomRead shared
  (res, newst) <- runAppState ctx (putRecord name record)
  _            <- atomReset newst shared
  _            <- N.send socket (encode $ res)
  return ()

processRequest _ _ Shutdown = return ()
-- |
-- | Query
-- |


-- TODO: REFACTOR THAT INTO A COMMON PATTERN
runQuery :: TVar DBContext
            -> DbName
            -> SelectQuery
            -> IO (DbErrorMonad DbResult)

runQuery shared dbName FetchAll = do
  ctx <- atomRead shared
  (res, newst) <- runAppState ctx (scan dbName EntireKeyspace Record appendFold)
  atomReset newst shared
  return (DbResults <$> res)

runQuery _ _ other = do
  _ <- print $ other
  return (Right EmptyRes)




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
startClientAcceptor startedMVar port _ = do
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
      N.send serverSocket (encode emptyResult)
      return ()

    (Left err)       -> do
      print ("Can't decode message: " ++ (show err))
      receiveLoop serverSocket shared

    (Right request)  -> do
      print ("Request received: " ++ (show request))
      processRequest serverSocket shared request
      receiveLoop serverSocket shared

atomRead :: TVar a -> IO a
atomRead = atomically . readTVar

swap :: (b -> b) -> TVar b -> IO ()
swap fn x = atomically $ readTVar x >>= writeTVar x . fn

atomReset :: b -> TVar b -> IO ()
atomReset newv x = atomically $ writeTVar x newv
