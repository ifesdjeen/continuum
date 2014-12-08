{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Storage.Test where

import qualified Continuum.Cluster as Server
import           Control.Monad.IO.Class ( liftIO )
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

runner :: DbState a -> IO (DbErrorMonad a)
runner = Server.withTmpStorage testDbPath testDbContext cleanup

main :: IO ()
main =  hspec $ do
  -- let scantdb = scan testDbName

  describe "Basic DB Functionality" $ do
    it "should put items into the database and retrieve them" $  do

      res <- runner $ do
        _   <- createDatabase testDbName testSchema
        _   <- putRecordTdb $ makeRecord 100 [("a", DbInt 1)]
        _   <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]
        ctx <- readT
        liftIO $ scan ctx testDbName (SingleKey 123) Record appendFold

      res `shouldBe` Right [RecordRes $ makeRecord 123 [("a", DbInt 1)]]


  describe "Scans" $ do

    it "Single Key Scan " $  do
      res <- runner $ do
        _ <- createDatabase testDbName testSchema

        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3)]

        ctx <- readT
        liftIO $ scan ctx testDbName (SingleKey 123) Record appendFold

      res `shouldBe` Right [RecordRes $ makeRecord 123 [("a", DbInt 1)],
                            RecordRes $ makeRecord 123 [("a", DbInt 2)],
                            RecordRes $ makeRecord 123 [("a", DbInt 3)]]


    it "Key Range (inclusive Range)" $  do
      res <- runner $ do
        _ <- createDatabase testDbName testSchema

        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 124 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 125 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 555 [("a", DbInt 3)]

        ctx <- readT
        liftIO $ scan ctx testDbName (KeyRange 123 456) Record appendFold

      res `shouldBe` Right [RecordRes $ makeRecord 123 [("a", DbInt 1)],
                            RecordRes $ makeRecord 124 [("a", DbInt 2)],
                            RecordRes $ makeRecord 125 [("a", DbInt 3)],

                            RecordRes $ makeRecord 456 [("a", DbInt 1)],
                            RecordRes $ makeRecord 456 [("a", DbInt 2)],
                            RecordRes $ makeRecord 456 [("a", DbInt 3)]]

    it "Key Range (inclusive range), when there're records both before and after" $  do
      res <- runner $ do
        _ <- createDatabase testDbName testSchema

        _ <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 124 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 125 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 1)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 2)]
        _ <- putRecordTdb $ makeRecord 456 [("a", DbInt 3)]

        _ <- putRecordTdb $ makeRecord 700 [("a", DbInt 3)]

        ctx <- readT
        liftIO $ scan ctx testDbName (KeyRange 300 456) Record appendFold

      res `shouldBe` Right [RecordRes $ makeRecord 456 [("a", DbInt 1)],
                            RecordRes $ makeRecord 456 [("a", DbInt 2)],
                            RecordRes $ makeRecord 456 [("a", DbInt 3)]]


  --   -- it "Open End" $ do -- TODO

  describe "Snapshotting" $ do

    it "should create snapshots after each 10 records" $  do
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ [1..100] (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1)])

        readChunks EntireKeyspace

      res `shouldBe` (Right (take 10 $ [(KeyRes $ 10 * x + 1) | x <- [0..]]))


  describe "Queries" $ do
    it "should run FetchAll query" $ do
      let records = take 1000 [makeRecord i [("a", DbInt $ i + 10)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        ctx <- readT
        liftIO $ scan ctx testDbName EntireKeyspace Record (queryStep FetchAll)

      res `shouldBe` (Right (ListStep $ (map RecordRes records)))

    -- it "should run FetchAll
    -- FetchAll with limits
    -- Group
    -- Min
    -- Max
    -- Combinations

  describe "Stack Allocations / Thunk Leaks" $ do
    it "should iterate over a large amount of records" $  do
      let res = runner $ do
            _ <- createDatabase testDbName testSchema

            forM_ [1..1000000]
              (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1),
                                                  ("b", DbString "1")])

            ctx <- readT
            liftIO $ scan ctx testDbName EntireKeyspace (Field "a") (queryStep Count)

      res `shouldReturn` (Right (CountStep 1000000))


    -- it "should iterate " $  do
    --   let res = runner testSchema $ do

testDbPath :: String
testDbPath = "/tmp/haskell-leveldb-tests"

testDbName :: ByteString
testDbName = "testdb"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDbPath) >> system ("mkdir " ++ testDbPath) >> return ()

putRecordTdb :: DbRecord -> DbState DbResult
putRecordTdb = putRecord testDbName
