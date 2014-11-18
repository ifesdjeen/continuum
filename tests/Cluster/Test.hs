{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Cluster.Test where

import           Continuum.Cluster
import           Data.ByteString                ( ByteString )
import           System.Process                 ( system )
import           Control.Concurrent             ( forkIO, newEmptyMVar, putMVar, takeMVar, MVar )
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Exception.Base         ( bracket )
import           Test.Hspec

import qualified Continuum.Client.Base          as Client

import           Debug.Trace                    ( trace )

exec = Client.executeQuery

main :: IO ()
main = hspec $ around_ withRunningServer $ do
  describe "Insert and Query" $ do
    it "should insert and retrieve data" $ do
      client  <- Client.connect "127.0.0.1" testPort
      _       <- exec client (Client.CreateDb dbName dbSchema)
      let mkRec ts i s =
            Client.makeRecord ts [ ("intField",    Client.DbInt i)
                                 , ("stringField", Client.DbString s)]

      a  <- exec client (Client.Insert dbName (mkRec 112233 1 "one"))
      b  <- exec client (Client.Insert dbName (mkRec 223344 2 "two"))

      let res = exec client (Client.FetchAll dbName)
      res `shouldReturn` (Right (Client.DbResults [Client.RecordRes $ mkRec 112233 1 "one"
                                                  ,Client.RecordRes $ mkRec 223344 2 "two"]))
      Client.disconnect client
      return ()


startServer :: IO (MVar ())
startServer = do
  startedVar <- newEmptyMVar
  doneVar    <- newEmptyMVar
  _          <- forkIO $ startNode startedVar doneVar testDBPath testPort
  _          <- takeMVar startedVar
  return doneVar

stopServer :: MVar () -> IO ()
stopServer doneVar = do
  client     <- Client.connect "127.0.0.1" testPort
  a          <- Client.sendRequest client Client.Shutdown
  _          <- takeMVar doneVar
  _          <- system ("rm -fr " ++ testDBPath) >> return ()
  _          <- Client.disconnect client
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
