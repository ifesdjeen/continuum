{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Storage.Test where

import qualified Continuum.Server           as Server
import qualified Data.Map                   as Map
import qualified Continuum.Storage.Parallel as PS

import           Data.ByteString          ( ByteString )
import           Control.Monad            ( forM_ )
import           System.Process           ( system )
import           Continuum.Storage.Engine ( createDatabase, readChunks, putRecord )

import           Continuum.Types
import           Continuum.Context
import           Test.Hspec
import           Control.Monad.IO.Class         ( liftIO )
import           Debug.Trace

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt) ]

testDbContext :: Integer -> DbContext
testDbContext sa = defaultDbContext { snapshotAfter = sa }

runner :: DbState a -> IO (DbErrorMonad a)
runner = Server.withTmpStorage testDbPath (testDbContext 10) cleanup

main :: IO ()
main =  hspec $ do
  -- let scantdb = scan testDbName

  describe "Basic DB Functionality" $ do

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

        PS.parallelScan testDbName (KeyRange 123 456) Record FetchAll

      res `shouldBe` (Right $ ListResult [makeRecord 123 [("a", DbInt 1)],
                                          makeRecord 124 [("a", DbInt 2)],
                                          makeRecord 125 [("a", DbInt 3)],

                                          makeRecord 456 [("a", DbInt 1)],
                                          makeRecord 456 [("a", DbInt 2)],
                                          makeRecord 456 [("a", DbInt 3)]])

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

        PS.parallelScan testDbName (KeyRange 300 456) Record FetchAll

      res `shouldBe` (Right $ ListResult [makeRecord 456 [("a", DbInt 1)],
                                          makeRecord 456 [("a", DbInt 2)],
                                          makeRecord 456 [("a", DbInt 3)]])

  describe "Queries" $ do
    it "should run FetchAll query" $ do
      let records = take 100 [makeRecord i [("a", DbInt $ i + 10)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb
        PS.parallelScan testDbName EntireKeyspace Record FetchAll

      res `shouldBe` (Right $ ListResult records)

    it "should run Min query" $ do
      let records = take 100 [makeRecord i [("a", DbInt $ i + 10)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        PS.parallelScan testDbName EntireKeyspace Record (Min "a")

      res `shouldBe` (Right $ ValueRes $ DbInt 10)

    it "should run Multi query" $ do
      let records = take 100 [makeRecord i [("a", DbInt $ i + 10)] | i <- [0..]]

      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        PS.parallelScan testDbName EntireKeyspace Record (Multi [("min", Min "a"),
                                                                 ("avg", Mean "a")])

      res `shouldBe` (Right $ MultiResult $ Map.fromList [("avg",ValueRes (DbDouble 59.5)),
                                                          ("min",ValueRes (DbInt 10))])


    it "should run Field Group query" $ do

      let groupSchema = makeSchema [ ("a", DbtInt), ("b", DbtString) ]
          recordsA    = take 2 [makeRecord i [("a", DbInt $ i + 10),
                                              ("b", DbString "a")] | i <- [1..]]
          recordsB    = take 3 [makeRecord (i + 50) [("a", DbInt $ i + 10),
                                                     ("b", DbString "b")] | i <- [1..]]

      res <- runner $ do
        _ <- createDatabase testDbName groupSchema

        _ <- forM_ recordsA putRecordTdb
        _ <- forM_ recordsB putRecordTdb

        PS.parallelScan testDbName EntireKeyspace Record (FieldGroup "b" FetchAll)

      res `shouldBe` (Right $ MapResult $ Map.fromList [(DbString "a", ListResult recordsA),
                                                        (DbString "b", ListResult recordsB)])


    it "should run Time Group query" $ do

      let groupSchema = makeSchema [ ("a", DbtInt), ("b", DbtString) ]
          recordsA    = take 2 [makeRecord (1420471452271 + i) [("a", DbInt $ i + 10),
                                                                ("b", DbString "a")] | i <- [1..]]
          recordsB    = take 3 [makeRecord (1420471452271 + 1000 + i) [("a", DbInt $ i + 10),
                                                                       ("b", DbString "b")] | i <- [1..]]

      res <- runner $ do
        _ <- createDatabase testDbName groupSchema

        _ <- forM_ recordsA putRecordTdb
        _ <- forM_ recordsB putRecordTdb

        PS.parallelScan testDbName EntireKeyspace Record (TimeGroup (Seconds 1) FetchAll)

      res `shouldBe` (Right $ MapResult $ Map.fromList [(DbInt 1420471452000, ListResult recordsA),
                                                        (DbInt 1420471453000, ListResult recordsB)])

    it "should run nested Group queries" $ do

      let groupSchema = makeSchema [ ("a", DbtInt), ("b", DbtString) ]
          records     = [makeRecord (1420471452271) [("a", DbInt 1),
                                                     ("b", DbString "a")],
                         makeRecord (1420471452271) [("a", DbInt 2),
                                                     ("b", DbString "b")],
                         makeRecord (1420471453271) [("a", DbInt 3),
                                                     ("b", DbString "a")],
                         makeRecord (1420471453271) [("a", DbInt 4),
                                                     ("b", DbString "b")]]

      res <- runner $ do
        _ <- createDatabase testDbName groupSchema

        _ <- forM_ records putRecordTdb

        PS.parallelScan testDbName EntireKeyspace Record (TimeFieldGroup "b" (Seconds 1) (Min "a"))

      res `shouldBe` (Right $ MapResult $ Map.fromList
                      [(DbList [DbString "a",DbInt 1420471452000],
                        ValueRes $ DbInt 1),
                       (DbList [DbString "a",DbInt 1420471453000],
                        ValueRes $ DbInt 3),
                       (DbList [DbString "b",DbInt 1420471452000],
                        ValueRes $ DbInt 2),
                       (DbList [DbString "b",DbInt 1420471453000],
                        ValueRes $ DbInt 4)])

    it "should run Mean query" $ do
      let records = take 100 [makeRecord i [("a", DbInt $ i)] | i <- [0..]]
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ records putRecordTdb

        PS.parallelScan testDbName EntireKeyspace Record (Mean "a")

      res `shouldBe` (Right $ ValueRes $ DbDouble 49.5)

  describe "Snapshotting" $ do

    it "should create snapshots after each 10 records" $  do
      res <- runner $ do
        _ <- createDatabase testDbName testSchema
        _ <- forM_ [1..100] (\i -> putRecordTdb $ makeRecord i [("a", DbInt 1)])

        readChunks EntireKeyspace

      res `shouldBe` (Right (take 10 $ [(10 * x + 1) | x <- [0..]]))

    -- it "should run FetchAll
    -- FetchAll with limits
    -- Group
    -- Min
    -- Max
    -- Combinations

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
