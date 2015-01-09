{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import qualified Continuum.Server           as Server
import qualified Continuum.Storage.Parallel as PS

import           Data.ByteString          ( ByteString )
import           Control.Monad            ( forM_ )
import           Continuum.Storage.Engine ( createDatabase, readChunks, putRecord )

import           Continuum.Common.Types
import           Continuum.Context
import           Debug.Trace

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt) ]

testDbContext :: Integer -> DbContext
testDbContext sa = defaultDbContext { snapshotAfter = sa }

runner :: DbState a -> IO (DbErrorMonad a)
runner = Server.withTmpStorage testDbPath (testDbContext 10) cleanup

main :: IO ()
main = do
  _ <- Server.withTmpStorage testDbPath (testDbContext 10000) cleanup $ do
    _ <- createDatabase testDbName testSchema
    _ <- forM_ [1..1000000]
         (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1)])
    PS.parallelScan testDbName EntireKeyspace (Field "a") Count
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
