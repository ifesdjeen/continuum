{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Storage.Test where

import Control.Monad (forM_)
import Continuum.Aggregation
import Continuum.Folds
import Continuum.Serialization
import Continuum.Storage
import Continuum.Types

import System.Process(system)
import Test.Hspec

testSchema :: DbSchema
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
      res `shouldReturn` (Right [(makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))])])

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

      res `shouldReturn` (Right [ (makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))])
                                ,(makeRecord 123 [("a", (DbInt 2)), ("b", (DbString "2"))])
                                ,(makeRecord 123 [("a", (DbInt 3)), ("b", (DbString "3"))])])

    it "setup" $ cleanup >>= shouldReturn (return())

    it "should return inclusive range of timestamps" $  do
      let res = runApp testDBPath testSchema $ do
            putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "3"))]

            putRecord $ makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))]

            putRecord $ makeRecord 555 [("a", (DbInt 3)), ("b", (DbString "3"))]

            findRange 123 456

      res `shouldReturn` (Right [ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
                                ,makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "2"))]
                                ,makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "3"))]
                                ,makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))]
                                ,makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))]
                                ,makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))]
                                ])

    it "should scan a range of times that do not directly include the record at hand inclusive range of timestamps" $  do
      let res = runApp testDBPath testSchema $ do
            putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "3"))]

            putRecord $ makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))]
            putRecord $ makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))]
            putRecord $ makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))]

            putRecord $ makeRecord 700 [("a", (DbInt 3)), ("b", (DbString "3"))]

            findRange 300 456

      res `shouldReturn` (Right [makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))]
                                ,makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))]
                                ,makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))]
                                ])


    it "should iterate over a large amount of records" $  do
      let res = runApp testDBPath testSchema $ do
            forM_ [1..1000000]
              (\i -> putRecord $ makeRecord i [("a", (DbInt 1)), ("b", (DbString "1"))])
            aggregateAllByField "status" countFold
      res `shouldReturn` (Right 1000000)
    -- it "should iterate " $  do
    --   let res = runApp testDBPath testSchema $ do

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()


-- TODO: port tests to quickcheck
---
