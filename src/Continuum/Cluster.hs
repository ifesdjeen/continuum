{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE RecordWildCards #-}

module Continuum.Cluster where

import           Continuum.Types
import           Continuum.Storage
import           Continuum.ParallelStorage
import           Continuum.Folds

import           Control.Concurrent             ( putMVar, MVar )
import           Control.Concurrent.STM

import           Control.Monad.IO.Class         ( liftIO )
import           Control.Monad.State.Strict     ( runStateT, evalStateT, execStateT, StateT(..) )
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
                  -> Request
                  -> StateT DbContext IO ()
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
            -> AppState DbResult

runQuery dbName query = parallelScan dbName EntireKeyspace Record query

withTmpStorage :: String
               -> DbContext
               -> IO ()
               -> AppState a
               -> IO (DbErrorMonad a)
withTmpStorage path context cleanup subsystem =
  bracket (startStorage path context)
          (\_ -> return ())
  (runStateT subsystem) >>= (\(res,state) -> (stopStorage state) >> cleanup >> return res)


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

-- withClientAcceptor :: MVar ()
--                       -> MVar ()
--                       -> String
--                       -> TVar DbContext
--                       -> IO ()

-- withClientAcceptor startedMVar doneMVar port shared =
--   bracket (startClientAcceptor startedMVar port shared)
--           (stopClientAcceptor doneMVar)
--           (\(serverSocket, _) -> receiveLoop serverSocket shared)

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
          (\(context, (serverSocket, _)) -> (evalStateT (receiveLoop serverSocket) context ) >> return ())
  -- withStorage path defaultDbContext $
  --   withClientAcceptor startedMVar doneMVar port


emptyResult :: DbErrorMonad DbResult
emptyResult = Right EmptyRes

receiveLoop :: N.Socket N.Rep -> StateT DbContext IO ()
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

-- atomRead :: TVar a -> IO a
-- atomRead = atomically . readTVar

-- swap :: (b -> b) -> TVar b -> IO ()
-- swap fn x = atomically $ readTVar x >>= writeTVar x . fn

-- atomReset :: b -> TVar b -> IO ()
-- atomReset newv x = atomically $ writeTVar x newv
