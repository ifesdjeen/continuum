{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE RecordWildCards #-}

module Continuum.Server where

import           Continuum.Context
import           Continuum.Types
import           Continuum.Storage.Engine
import           Continuum.Storage.Parallel

import           Control.Concurrent             ( putMVar, MVar )
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Monad.State.Strict     ( runStateT, evalStateT )
import           Control.Exception.Base         ( bracket )
import           Data.Serialize                 ( encode, decode )
import           Continuum.Http.Server          ( runWebServer )

import qualified Nanomsg as N

type Socket = N.Socket N.Rep

-- |
-- | Reuqest Processing
-- |

processRequest :: Socket
                  -> Request
                  -> DbDryRun
-- TODO: GET RID OF THAT
processRequest socket (Select dbName query) = do
  resp  <- runQuery dbName query
  _     <- liftIO $ N.send socket (encode $ resp)
  return ()

processRequest socket (CreateDb name schema) = do
  res          <- createDatabase name schema
  _            <- liftIO $ N.send socket (encode $ res)
  return ()


processRequest socket (Insert name record) = do
  res          <- putRecord name record
  _            <- liftIO $ N.send socket (encode $ res)
  return ()

processRequest _ Shutdown = return ()
-- |
-- | Query
-- |


runQuery :: DbName
            -> SelectQuery
            -> DbState DbResult
runQuery dbName query = parallelScan dbName EntireKeyspace Record query

withTmpStorage :: String
               -> DbContext
               -> IO ()
               -> DbState a
               -> IO (DbErrorMonad a)
withTmpStorage path context cleanup subsystem = do
  bracket (startStorage path context)
          stop
          runner
  where stop _ = return ()
        runner ctx = do
          ctxTVar <- atomInit ctx
          (res,_) <- runStateT subsystem ctxTVar
          ctx'    <- atomRead ctxTVar
          _       <- stopStorage ctx'
          _       <- cleanup
          return res

startClientAcceptor :: MVar ()
                       -> String
                       -> DbContext
                       -> IO (DbContext, (N.Socket N.Rep, N.Endpoint)) -- Probably that all should be dbcontext??
startClientAcceptor startedMVar port context = do
  serverSocket <- N.socket N.Rep
  endpoint     <- N.bind serverSocket ("tcp://*:" ++ port)

  _            <- putMVar startedMVar ()

  return (context, (serverSocket, endpoint))

stopClientAcceptor :: MVar () -> (N.Socket N.Rep, N.Endpoint) -> IO ()
stopClientAcceptor doneMVar (serverSocket, endpoint) = do
  _    <- N.shutdown serverSocket endpoint
  _    <- N.close serverSocket

  _    <- putMVar doneMVar ()

  return ()

startNode :: MVar ()
             -> MVar ()
             -> String -> String
             -> IO ()
startNode startedMVar doneMVar path port = do
  bracket ( (startStorage path defaultDbContext) >>= (startClientAcceptor startedMVar port) )
          ( \(x,y) -> do
               _ <- stopStorage x
               _ <- stopClientAcceptor doneMVar y
               return ())
          runner
  where runner (context, (serverSocket, _)) = do
          ctxTVar <- atomInit context
          _       <- runWebServer ctxTVar
          _       <- evalStateT (receiveLoop serverSocket) ctxTVar
          return ()

emptyResult :: DbErrorMonad DbResult
emptyResult = Right EmptyRes

receiveLoop :: N.Socket N.Rep -> DbDryRun
receiveLoop serverSocket = do
  received <- liftIO $ N.recv serverSocket

  case (decode received :: Either String Request) of
    (Right Shutdown) -> do
      _ <- liftIO $ N.send serverSocket (encode emptyResult)
      return ()

    (Left err)       -> do
      _ <- liftIO $ print ("Can't decode message: " ++ (show err))
      receiveLoop serverSocket

    (Right request)  -> do
      _ <- liftIO $ print ("Request received: " ++ (show request))
      processRequest serverSocket request
      receiveLoop serverSocket
