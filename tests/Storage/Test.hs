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
testSchema = makeSchema [ ("a", DbtInt) ]

testDbContext :: DbContext
testDbContext = defaultDbContext { snapshotAfter = 10 }

runner :: String -> AppState a -> IO (DbErrorMonad a)
runner str op = Server.withTmpStorage str testDbContext cleanup $ \shared -> do
  ctx <- Server.atomRead shared
  (res, newst) <- runAppState ctx op
  Server.atomReset newst shared
  return res

main :: IO ()
main =  hspec $ do
  let scantdb = scan testDBName
  describe "Basic DB Functionality" $ do
    it "should put items into the database and retrieve them" $  do

      res <- runner testDBPath $ do
        _ <- createDatabase testDBName testSchema
        _ <- putRecordTdb $ makeRecord 100 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]

        scantdb (SingleKey 123) Record appendFold

      res `shouldBe` Right [RecordRes $ makeRecord 123 [("a", DbInt 1)]]


  describe "Scans" $ do

    it "Single Key Scan " $  do
      res <- runner testDBPath $ do
        _ <- createDatabase testDBName testSchema

        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3)]

        scantdb (SingleKey 123) Record appendFold

      res `shouldBe` Right [RecordRes $ makeRecord 123 [("a", DbInt 1)],
                            RecordRes $ makeRecord 123 [("a", DbInt 2)],
                            RecordRes $ makeRecord 123 [("a", DbInt 3)]]



    it "Key Range (inclusive Range)" $  do
      res <- runner testDBPath $ do
        _ <- createDatabase testDBName testSchema

        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 124 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 125 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 555 [("a", DbInt 3)]

        scantdb (KeyRange 123 456) Record appendFold

      res `shouldBe` Right [RecordRes $ makeRecord 123 [("a", DbInt 1)],
                            RecordRes $ makeRecord 124 [("a", DbInt 2)],
                            RecordRes $ makeRecord 125 [("a", DbInt 3)],

                            RecordRes $ makeRecord 456 [("a", DbInt 1)],
                            RecordRes $ makeRecord 456 [("a", DbInt 2)],
                            RecordRes $ makeRecord 456 [("a", DbInt 3)]]

    it "Key Range (inclusive range), when there're records both before and after" $  do
      let res = runner testDBPath $ do
            _ <- createDatabase testDBName testSchema

            _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]
            _ <- putRecordTdb $ makeRecord 124 [("a", DbInt 2)]
            _ <- putRecordTdb $ makeRecord 125 [("a", DbInt 3)]

            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1)]
            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2)]
            _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3)]

            _ <- putRecordTdb $ makeRecord 700 [("a", DbInt 3)]

            scantdb (KeyRange 300 456) Record appendFold

      res `shouldReturn` Right [RecordRes $ makeRecord 456 [("a", DbInt 1)],
                                RecordRes $ makeRecord 456 [("a", DbInt 2)],
                                RecordRes $ makeRecord 456 [("a", DbInt 3)]]


    -- it "Open End" $ do -- TODO

  describe "Snapshotting" $ do

    it "should create snapshots after each 10 records" $  do
      let res = runner testDBPath $ do
            _ <- createDatabase testDBName testSchema
            forM_ [1..100]
              (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1),
                                                  ("b", DbString "1")])
            readChunks EntireKeyspace
      res `shouldReturn` (Right (take 10 $ [(KeyRes $ 10 * x + 1) | x <- [0..]]))


  -- describe "Queries" $ do
    -- FetchAll
    -- FetchAll with limits
    -- Group
    -- Min
    -- Max
    -- Combinations

  describe "Stack Allocations / Thunk Leaks" $ do
    it "should iterate over a large amount of records" $  do
      let res = runner testDBPath $ do
            _ <- createDatabase testDBName testSchema

            forM_ [1..1000000]
              (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1),
                                                  ("b", DbString "1")])
            scantdb EntireKeyspace (Field "a") (queryStep Count)
      res `shouldReturn` (Right (CountStep 1000000))


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
