{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import qualified Continuum.Server           as Server
import qualified Continuum.Storage.Parallel as PS
import qualified Data.Time.Clock.POSIX      as Clock

import           Data.ByteString          ( ByteString )
import           Control.Monad            ( forM_ )
import           Continuum.Storage.Engine ( createDatabase, readChunks, putRecord )

import           Continuum.Types
import           Continuum.Context
import           Debug.Trace

import           Control.Monad.IO.Class         ( MonadIO(..), liftIO )

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt) ]

testDbContext :: Integer -> DbContext
testDbContext sa = defaultDbContext { snapshotAfter = sa }

runner :: DbState a -> IO (DbErrorMonad a)
runner = Server.withTmpStorage testDbPath (testDbContext 10) cleanup

main :: IO ()
main = do
  res <- Server.withTmpStorage testDbPath (testDbContext 100000) cleanup $ do
    -- _ <- createDatabase testDbName testSchema
    -- _ <- forM_ [1..1000000]
    --      (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1)])
    start <- liftIO $ Clock.getPOSIXTime
    res <- PS.parallelScan testDbName AllTime Key Count
    end <- liftIO $  Clock.getPOSIXTime
    liftIO $ print (end - start)
    return res
    -- PS.parallelScan testDbName EntireKeyspace (Field "a") Count
  print res
  return ()

testDbPath :: String
testDbPath = "/tmp/haskell-leveldb-tests"

testDbName :: ByteString
testDbName = "testdb"

cleanup :: IO ()
cleanup = do
  return ()

putRecordTdb :: DbRecord -> DbState DbResult
putRecordTdb = putRecord testDbName
