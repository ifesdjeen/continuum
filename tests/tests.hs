{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import Continuum.Serialization
import Continuum.Storage

import System.Process(system)
import Test.Hspec
import Test.Hspec.Expectations
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

import Control.Monad (liftM, void)
import Control.Monad.IO.Class (MonadIO (liftIO))
-- import Data.Default
-- import Database.LevelDB


import Control.Monad.Reader

testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)]
main :: IO ()
main =  hspec $ do


  describe "Basic DB Functionality" $ do
    it "setup" $ cleanup >>= shouldReturn (return())
    it "should put items into the database and retrieve them" $  do
      let res = runApp testDBPath testSchema $ do
            putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
            findByTimestamp 123
      res `shouldReturn` [(makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))])]

    it "setup" $ cleanup >>= shouldReturn (return())
    it "should retrieve items by given timestamp" $  do
      let res = runApp testDBPath testSchema $ do
            putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 123 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 123 [("a", (DbInt 3)), ("b", (DbString "3"))]

            putRecord $ makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))]

            findByTimestamp 123

      res `shouldReturn` [ (makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))])
                          ,(makeRecord 123 [("a", (DbInt 2)), ("b", (DbString "2"))])
                          ,(makeRecord 123 [("a", (DbInt 3)), ("b", (DbString "3"))])]

    it "setup" $ cleanup >>= shouldReturn (return())
    it "should return inclusive range of timestamps" $  do
      let res = runApp testDBPath testSchema $ do
            putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "3"))]

            putRecord $ makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))]

            findRange 123 456

      res `shouldReturn` [ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
                          ,makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "2"))]
                          ,makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "3"))]
                          ,makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))]
                          ,makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))]
                          ,makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))]
                          ]

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()
