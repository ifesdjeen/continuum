{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Storage.Test where

import qualified Continuum.Cluster      as Server
import qualified Data.Map                       as Map
import qualified Continuum.ParallelStorage as PS
import           Control.Monad.IO.Class ( liftIO )
import           Data.ByteString        ( ByteString )
import           Control.Monad          ( forM_ )
import           System.Process         ( system )
import           Continuum.Folds
import           Continuum.Storage
import           Continuum.Common.Types
import           Continuum.Context

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
    it "Key Range (single key)" $ do
      res <- runner $ do
        _   <- createDatabase testDbName testSchema
        _   <- putRecordTdb $ makeRecord 100 [("a", DbInt 1)]
        _   <- putRecordTdb $ makeRecord 123 [("a", DbInt 1)]

        ctx <- readT
        liftIO $ scan ctx testDbName (SingleKey 123) Record appendFold
      res `shouldBe` Right [makeRecord 123 [("a", DbInt 1)]]

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

      res `shouldBe` Right [makeRecord 123 [("a", DbInt 1)],
                            makeRecord 123 [("a", DbInt 2)],
                            makeRecord 123 [("a", DbInt 3)]]


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

      res `shouldBe` Right [makeRecord 123 [("a", DbInt 1)],
                            makeRecord 124 [("a", DbInt 2)],
                            makeRecord 125 [("a", DbInt 3)],

                            makeRecord 456 [("a", DbInt 1)],
                            makeRecord 456 [("a", DbInt 2)],
                            makeRecord 456 [("a", DbInt 3)]]

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

      res `shouldBe` Right [makeRecord 456 [("a", DbInt 1)],
                            makeRecord 456 [("a", DbInt 2)],
                            makeRecord 456 [("a", DbInt 3)]]


  describe "Snapshotting" $ do

    it "should create snapshots after each 10 records" $  do
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ [1..100] (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1)])

        readChunks EntireKeyspace

      res `shouldBe` (Right (take 10 $ [(10 * x + 1) | x <- [0..]]))


  describe "Queries" $ do
    it "should run FetchAll query" $ do
      let records = take 1000 [makeRecord i [("a", DbInt $ i + 10)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        ctx <- readT
        liftIO $ scan ctx testDbName EntireKeyspace Record (queryStep FetchAll)

      res `shouldBe` (Right $ ListStep records)

    it "should run Min query" $ do
      let records = take 100 [makeRecord i [("a", DbInt $ i + 10)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        ctx <- readT

        liftIO $ scan ctx testDbName EntireKeyspace Record (queryStep (Min "a"))

      res `shouldBe` (Right $ MinStep $ DbInt 10)

    it "should run Multi query" $ do
      let records = take 100 [makeRecord i [("a", DbInt $ i + 10)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        ctx <- readT

        liftIO $ scan ctx testDbName EntireKeyspace Record (queryStep (Multi [("min", Min "a"),
                                                                              ("avg", Avg "a")
                                                                              ]))

      res `shouldBe` (Right $ MinStep $ DbInt 10)


    it "should run Group query" $ do

      let groupSchema = makeSchema [ ("a", DbtInt), ("b", DbtString) ]
          recordsA    = take 2 [makeRecord i [("a", DbInt $ i + 10),
                                              ("b", DbString "a")] | i <- [1..]]
          recordsB    = take 3 [makeRecord (i + 50) [("a", DbInt $ i + 10),
                                                     ("b", DbString "b")] | i <- [1..]]

      res <- runner $ do
        _ <- createDatabase testDbName groupSchema

        _ <- forM_ recordsA putRecordTdb
        _ <- forM_ recordsB putRecordTdb

        ctx <- readT
        liftIO $ scan ctx testDbName EntireKeyspace Record (queryStep (Group "b" FetchAll))

      res `shouldBe` (Right $ GroupStep $ Map.fromList [(DbString "a", ListStep recordsA),
                                                        (DbString "b", ListStep recordsB)])

    it "Parallel " $ do
      let records = take 100 [makeRecord i [("a", DbInt $ i)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        PS.parallelScan testDbName EntireKeyspace Record (Avg "a")

      res `shouldBe` (Right $ ValueRes $ DbDouble 49.5)

    -- it "should run FetchAll
    -- FetchAll with limits
    -- Group
    -- Min
    -- Max
    -- Combinations

  -- Slow test
  -- describe "Stack Allocations / Thunk Leaks" $ do
  --   it "should iterate over a large amount of records" $  do
  --     let res = runner $ do
  --           _ <- createDatabase testDbName testSchema

  --           forM_ [1..1000000]
  --             (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1),
  --                                                 ("b", DbString "1")])

  --           ctx <- readT
  --           liftIO $ scan ctx testDbName EntireKeyspace (Field "a") (queryStep Count)

  --     res `shouldReturn` (Right (CountStep 1000000))


    -- it "should iterate " $  do
    --   let res = runner testSchema $ do

testDbPath :: String
testDbPath = "/tmp/haskell-leveldb-tests"

testDbName :: ByteString
testDbName = "testdb"

cleanup :: IO ()
cleanup = do
  system ("rm -fr " ++ testDbPath) >> return () -- system ("mkdir " ++ testDbPath) >>

putRecordTdb :: DbRecord -> DbState DbResult
putRecordTdb = putRecord testDbName
