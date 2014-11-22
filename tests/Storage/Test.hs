{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Storage.Test where

import qualified Continuum.Cluster as Server
import           Continuum.Storage    ( runAppState )
import           Data.ByteString      ( ByteString )
import           Control.Monad        ( forM_ )
import           System.Process       ( system )
import           Continuum.Folds
import           Continuum.Storage
import           Continuum.Types

import           Test.Hspec

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)]

runner :: String -> AppState a -> IO (DbErrorMonad a)
runner str op = Server.withTmpStorage str cleanup $ \shared -> do
  ctx <- Server.atomRead shared
  (res, newst) <- runAppState ctx op
  Server.atomReset newst shared
  return res

main :: IO ()
main =  hspec $ do
  let scantdb = scan testDBName
  describe "Basic DB Functionality" $ do
    it "should put items into the database and retrieve them" $  do
      let res = runner testDBPath $ do
            _ <- createDatabase testDBName testSchema

            _ <- putRecordTdb $ makeRecord 123 [("a", (DbInt 1)),
                                                ("b", (DbString "1"))]

            scantdb (SingleKey 123) Record appendFold

      res `shouldReturn` Right [RecordRes $
                                makeRecord 123 [("a", DbInt 1),
                                                ("b", DbString "1")]]


    it "should retrieve items by given timestamp" $  do
      let res = runner testDBPath $ do
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

            scantdb (SingleKey 123) Record appendFold

      res `shouldReturn` Right [RecordRes $
                                makeRecord 123 [("a", DbInt 1),
                                                ("b", DbString "1")],
                                RecordRes $
                                makeRecord 123 [("a", DbInt 2),
                                                ("b", DbString "2")],
                                RecordRes $
                                makeRecord 123 [("a", DbInt 3),
                                                ("b", DbString "3")]]



    it "should return inclusive range of timestamps" $  do
      let res = runner testDBPath $ do
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

            scantdb (KeyRange 123 456) Record appendFold

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



    it "should scantdb a range of times that do not directly include the record at hand inclusive range of timestamps" $  do
      let res = runner testDBPath $ do
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

            scantdb (KeyRange 300 456) Record appendFold

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



    it "should iterate over a large amount of records" $  do
      let res = runner testDBPath $ do
            _ <- createDatabase testDBName testSchema

            forM_ [1..1000000]
              (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1),
                                                  ("b", DbString "1")])
            scantdb EntireKeyspace (Field "a") countFold
      res `shouldReturn` (Right 1000000)



    -- it "should iterate " $  do
    --   let res = runner testDBPath testSchema $ do

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

testDBName :: ByteString
testDBName = "testdb"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> system ("mkdir " ++ testDBPath) >> return ()

putRecordTdb :: DbRecord -> AppState DbResult
putRecordTdb = putRecord testDBName
