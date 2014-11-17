{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Storage.Test where

import           Data.ByteString  ( ByteString )
import           Control.Monad    ( forM_ )
import           System.Process   ( system )
import           Continuum.Folds
import           Continuum.Storage
import           Continuum.Types

import           Test.Hspec

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)]

main :: IO ()
main =  hspec $ do

  describe "Basic DB Functionality" $ do
    it "setup" $ cleanup >>= shouldReturn (return())

    it "should put items into the database and retrieve them" $  do
      let res = runApp testDBPath $ do
            _ <- createDatabase testDBName testSchema

            _ <- putRecordTdb $ makeRecord 123 [("a", (DbInt 1)),
                                                ("b", (DbString "1"))]

            scantdb (TsSingleKey 123) Record appendFold
      res `shouldReturn` Right [RecordRes $
                                makeRecord 123 [("a", DbInt 1),
                                                ("b", DbString "1")]]

    it "setup" $ cleanup >>= shouldReturn (return())
    it "should retrieve items by given timestamp" $  do
      let res = runApp testDBPath $ do
            _ <- createDatabase testDBName testSchema

            _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1),
                                                ("b", DbString "1")]
            _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 2),
                                                ("b", DbString "2")]
            _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 3),
                                                ("b", DbString "3")]

            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1),
                                                ("b", DbString "1")]
            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2),
                                                ("b", DbString "2")]
            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3),
                                                ("b", DbString "3")]

            scantdb (TsSingleKey 123) Record appendFold

      res `shouldReturn` Right [RecordRes $
                                makeRecord 123 [("a", DbInt 1),
                                                ("b", DbString "1")],
                                RecordRes $
                                makeRecord 123 [("a", DbInt 2),
                                                ("b", DbString "2")],
                                RecordRes $
                                makeRecord 123 [("a", DbInt 3),
                                                ("b", DbString "3")]]

    it "setup" $ cleanup >>= shouldReturn (return())

    it "should return inclusive range of timestamps" $  do
      let res = runApp testDBPath $ do
            _ <- createDatabase testDBName testSchema

            _ <- putRecordTdb $ makeRecord 123 [("a", (DbInt 1)),
                                                ("b", (DbString "1"))]
            _ <- putRecordTdb $ makeRecord 124 [("a", (DbInt 2)),
                                                ("b", (DbString "2"))]
            _ <- putRecordTdb $ makeRecord 125 [("a", (DbInt 3)),
                                                ("b", (DbString "3"))]

            _ <- putRecordTdb $ makeRecord 456 [("a", (DbInt 1)),
                                                ("b", (DbString "1"))]
            _ <- putRecordTdb $ makeRecord 456 [("a", (DbInt 2)),
                                                ("b", (DbString "2"))]
            _ <- putRecordTdb $ makeRecord 456 [("a", (DbInt 3)),
                                                ("b", (DbString "3"))]

            _ <- putRecordTdb $ makeRecord 555 [("a", (DbInt 3)),
                                                ("b", (DbString "3"))]

            scantdb (TsKeyRange 123 456) Record appendFold

      res `shouldReturn` Right [RecordRes $
                                makeRecord 123 [("a", DbInt 1),
                                                ("b", DbString "1")],
                                RecordRes $
                                makeRecord 124 [("a", DbInt 2),
                                                ("b", DbString "2")],
                                RecordRes $
                                makeRecord 125 [("a", DbInt 3),
                                                ("b", DbString "3")],
                                RecordRes $
                                makeRecord 456 [("a", DbInt 1),
                                                ("b", DbString "1")],
                                RecordRes $
                                makeRecord 456 [("a", DbInt 2),
                                                ("b", DbString "2")],
                                RecordRes $
                                makeRecord 456 [("a", DbInt 3),
                                                ("b", DbString "3")]]

    it "setup" $ cleanup >>= shouldReturn (return())

    it "should scantdb a range of times that do not directly include the record at hand inclusive range of timestamps" $  do
      let res = runApp testDBPath $ do
            _ <- createDatabase testDBName testSchema

            _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1),
                                                ("b", DbString "1")]
            _ <- putRecordTdb $ makeRecord 124 [("a", DbInt 2),
                                                ("b", DbString "2")]
            _ <- putRecordTdb $ makeRecord 125 [("a", DbInt 3),
                                                ("b", DbString "3")]

            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1),
                                                ("b", DbString "1")]
            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2),
                                                ("b", DbString "2")]
            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3),
                                                ("b", DbString "3")]

            _ <- putRecordTdb $ makeRecord 700 [("a", DbInt 3),
                                                ("b", DbString "3")]

            scantdb (TsKeyRange 300 456) Record appendFold

      res `shouldReturn` (Right [RecordRes $
                                 makeRecord 456 [("a", DbInt 1),
                                                 ("b", DbString "1")],
                                 RecordRes $
                                 makeRecord 456 [("a", DbInt 2),
                                                 ("b", DbString "2")],
                                 RecordRes $
                                 makeRecord 456 [("a", DbInt 3),
                                                 ("b", DbString "3")]
                                ])


    it "setup" $ cleanup >>= shouldReturn (return())
    it "should iterate over a large amount of records" $  do
      let res = runApp testDBPath $ do
            _ <- createDatabase testDBName testSchema

            forM_ [1..1000000]
              (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1),
                                                  ("b", DbString "1")])
            scantdb EntireKeyspace (Field "a") countFold
      res `shouldReturn` (Right 1000000)

    it "setup" $ cleanup >>= shouldReturn (return())

    -- it "should iterate " $  do
    --   let res = runApp testDBPath testSchema $ do

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

testDBName :: ByteString
testDBName = "testdb"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> system ("mkdir " ++ testDBPath) >> return ()

putRecordTdb :: DbRecord -> AppState DbResult
putRecordTdb = putRecord testDBName

scantdb = scan testDBName
-- TODO: port tests to quickcheck
---
