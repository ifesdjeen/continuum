{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.RecordStorageSpec where

import Continuum.Serialization.Record
import Continuum.Serialization.Schema ( makeSchema )
import qualified Continuum.Storage.RecordStorage as Storage
import Continuum.Types
import           Control.Monad.Catch
import Database.LevelDB.Base ( BatchOp(..), DB, withIter, write, open, defaultOptions, createIfMissing, destroy )
import           Database.LevelDB.Internal  (unsafeClose)
import           Data.ByteString                   ( ByteString )
import           System.Directory
import           System.IO.Temp
import           Data.Default
import Test.Hspec

import Continuum.Support.QuickCheck
import Test.QuickCheck.Monadic
import Test.QuickCheck.Property ( Property(..) )
import Test.QuickCheck

-- import Debug.Trace

data Rs = Rs DB FilePath

tupleToBatchOp :: (ByteString, ByteString) -> BatchOp
tupleToBatchOp (x,y) = Put x y

roundTrip :: (DbSchema, [DbRecord]) -> Property
roundTrip (schema, records) = monadicIO $ do
  r   <- run $ bracket initDB destroyDB $ \(Rs db _) -> do
    _   <- write db def (map (tupleToBatchOp . (encodeRecord schema 1)) records)
    res <- withIter db def (\iter -> do
                               Storage.toList $ Storage.entrySlice iter AllKeys Asc (decodeRecord Record schema))
    return res
  assert $ r == Right records

spec :: Spec
spec = do

  describe "Recorg Storage" $ do
    it "Can iterate over the items and append them to the array" $ do
      r <- bracket initDB destroyDB $ \(Rs db _) -> do
        write db def [ Put "a" "one"
                     , Put "b" "two"
                     , Put "c" "three"]
        res <- withIter db def (\iter -> do
                                   Storage.toList $ Storage.entrySlice iter AllKeys Asc Right)
        return res
      r `shouldBe` (Right [("a","one"),("b","two"),("c","three")])

    it "Can iterate over the decoded records" $ do
      let schema  = makeSchema [ ("a", DbtLong) ]
          records = [ makeRecord 1 [ ("a", DbLong 1) ]
                    , makeRecord 2 [ ("a", DbLong 2) ]]
      r <- bracket initDB destroyDB $ \(Rs db _) -> do

        write db def (map (tupleToBatchOp . (encodeRecord schema 1)) records)

        res <- withIter db def (\iter -> do
                                   Storage.toList $ Storage.entrySlice iter AllKeys Asc (decodeRecord Record schema))
        return res
      r `shouldBe` (Right records)

    it "Can iterate over the decoded records" $ do
      property $ roundTrip


--    it "Can iterate over the decoded records" $ do


initDB = do
  tmp <- getTemporaryDirectory
  dir <- createTempDirectory tmp "leveldb-streaming-tests"
  db  <- open dir defaultOptions { createIfMissing = True }
  return $ Rs db dir

destroyDB (Rs db dir) = unsafeClose db `finally` destroy dir defaultOptions
