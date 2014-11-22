{-# LANGUAGE OverloadedStrings #-}


module Main where

import Continuum.Cluster

import           Continuum.Client.Base
import           Continuum.Cluster

import           Data.ByteString                ( ByteString )
import           System.Process                 ( system )
import           Control.Concurrent             ( forkIO, newEmptyMVar, takeMVar, MVar )
import           Control.Exception.Base         ( bracket )

import           Debug.Trace                    ( trace )


main :: IO ()
main = withRunningServer $ do
  client  <- connect "127.0.0.1" testPort
  _       <- sendRequest client (CreateDb dbName dbSchema)

  _       <- sendRequest client (Insert dbName (mkRec 112233 1 "one"))
  _       <- sendRequest client (Insert dbName (mkRec 223344 2 "two"))

  res     <- exec client dbName FetchAll

  print $ res

  _       <- disconnect client
  return ()

  where mkRec ts i s = makeRecord ts [ ("intField",    DbInt i)
                                     , ("stringField", DbString s)]

-- auxilliary routines
-- -------------------

startServer :: IO (MVar ())
startServer = do
  startedVar <- newEmptyMVar
  doneVar    <- newEmptyMVar
  _          <- forkIO $ startNode startedVar doneVar testDBPath testPort
  _          <- takeMVar startedVar
  return doneVar

stopServer :: MVar () -> IO ()
stopServer doneVar = do
  client     <- connect "127.0.0.1" testPort
  _          <- sendRequest client Shutdown
  _          <- takeMVar doneVar
  _          <- system ("rm -fr " ++ testDBPath) >> return ()
  _          <- disconnect client
  return ()

withRunningServer :: IO () -> IO ()
withRunningServer action = do
  bracket startServer
          stopServer
          (const action)


testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

dbName :: ByteString
dbName = "testdb"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()

dbSchema :: DbSchema
dbSchema = makeSchema [ ("intField",    DbtInt)
                      , ("stringField", DbtString)]

testPort :: String
testPort = "5566"

exec :: ContinuumClient
        -> DbName
        -> SelectQuery
        -> IO (DbErrorMonad DbResult)
exec = executeQuery
