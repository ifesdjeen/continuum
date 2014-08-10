{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import System.Process(system)
import Test.Hspec
import Test.Hspec.Expectations
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

import Control.Monad (liftM, void)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Default
import Database.LevelDB


import Control.Monad.Reader

initializeDB :: MonadResource m => m DB
initializeDB =
  open "/tmp/leveltest" defaultOptions{ createIfMissing = True
                                      , cacheSize= 2048
                                      }

main :: IO ()
main =  hspec $ do
  liftIO $ cleanup

  describe "Basic DB Functionality" $ do
    it "should put items into the database and retrieve them" $  do
      db <- open testDBPath
            defaultOptions{ createIfMissing = True
                          , cacheSize= 2048
                                       -- , comparator = Just customComparator
                          }
      let ctx = makeContext db (makeSchema [("a", DbtInt), ("b", DbtString)]) (defaultReadOptions, defaultWriteOptions)


      liftIO $ (flip runStateT) ctx $ do
        putRecord (DbRecord
                   123
                   (Map.fromList [("a", (DbInt 1)), ("b", (DbString "1"))]))

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()
