{-# LANGUAGE OverloadedStrings #-}


module Main where

import Continuum.Cluster

import           System.Environment  ( getArgs )
import           Control.Concurrent  ( newEmptyMVar )

-- main :: IO ()
-- main = do
--   doneVar <- newEmptyMVar

--   -- TODO: add proper argument parsing
--   [path, host, port, seedHost, seedPort] <- getArgs

--   startNode doneVar path host port seedHost seedPort
--   return ()



import           Continuum.Cluster
import           Data.ByteString                ( ByteString )
import           System.Process                 ( system )
import           Control.Concurrent             ( forkIO, newEmptyMVar, putMVar, takeMVar, MVar )
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Exception.Base         ( bracket )
import qualified Continuum.Client.Base          as Client

import           Debug.Trace                    ( trace )


exec = Client.executeQuery

main = do
  a  <- startServer
  client  <- Client.connect "127.0.0.1" testPort
  c  <- Client.executeQuery client (Client.CreateDb dbName dbSchema)
  let mkRec ts i s =
        Client.makeRecord ts [ ("intField",    Client.DbInt i)
                             , ("stringField", Client.DbString s)]
  _  <- exec client (Client.Insert dbName (mkRec 112233 1 "one"))
  _  <- exec client (Client.Insert dbName (mkRec 223344 2 "two"))
  res <- exec client (Client.FetchAll dbName)
  print res
  stopServer a
  return ()

startServer :: IO (MVar ())
startServer = do
  startedVar <- liftIO newEmptyMVar
  doneVar    <- liftIO newEmptyMVar
  _          <- forkIO $ startNode startedVar doneVar testDBPath "127.0.0.1" testPort "-" "-"
  _          <- takeMVar startedVar
  return doneVar

stopServer :: MVar () -> IO ()
stopServer doneVar = do
  client     <- Client.connect "127.0.0.1" testPort
  a          <- Client.sendRequest client Client.Shutdown
  _          <- takeMVar doneVar
  _          <- system ("rm -fr " ++ testDBPath) >> return ()

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

dbSchema :: Client.DbSchema
dbSchema = Client.makeSchema [ ("intField",    Client.DbtInt)
                               , ("stringField", Client.DbtString)]

testPort :: String
testPort = "5566"
