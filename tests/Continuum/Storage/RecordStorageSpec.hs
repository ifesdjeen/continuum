{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.RecordStorageSpec where

import Continuum.Serialization.Record
import Continuum.Serialization.Schema ( makeSchema )
import Continuum.Storage.RecordStorage
import Continuum.Types
import           Control.Monad.Catch
import qualified Data.ByteString            as BS
import           Data.ByteString.Char8      (ByteString, singleton, unpack)
import           Data.Foldable              (foldMap)
import           Data.List
import           Data.Monoid
import           Database.LevelDB.Base
import           Database.LevelDB.Internal  (unsafeClose)
import           System.Directory
import           System.IO.Temp
import           Data.Default
import Test.Hspec

import Debug.Trace

data Rs = Rs DB FilePath

tupleToBatchOp (x,y) = Put x y
spec :: Spec
spec = do

  describe "Recorg Storage" $ do
    it "Can iterate over the items and append them to the array" $ do
      r <- bracket initDB destroyDB $ \(Rs db _) -> do
        write db def [ Put "a" "one"
                     , Put "b" "two"
                     , Put "c" "three"]
        res <- withIter db def (\iter -> do
          toList $ entrySlice iter (KeyRange {start = "a", end = (\i -> i `compare` "c")}) Asc (\x -> Right x))
        return res
      r `shouldBe` (Right [("a","one"),("b","two"),("c","three")])

    it "Can iterate over the decoded records" $ do
      r <- bracket initDB destroyDB $ \(Rs db _) -> do
        let schema = makeSchema [ ("a", DbtInt) ]
        write db def [ tupleToBatchOp $ encodeRecord schema $ makeRecord 1 1 [ ("a", DbLong 1) ]
                     , tupleToBatchOp $ encodeRecord schema $ makeRecord 2 2 [ ("a", DbLong 2) ]]
        res <- withIter db def (\iter -> do
          toList $ entrySlice iter (KeyRange {start = "a", end = (\i -> i `compare` "c")}) Asc (\x -> Right x))
        return res
      r `shouldBe` (Right [("a","one"),("b","two"),("c","three")])


  where
    initDB = do
        tmp <- getTemporaryDirectory
        dir <- createTempDirectory tmp "leveldb-streaming-tests"
        db  <- open dir defaultOptions { createIfMissing = True }
        -- write db def
        --   . map ( \ c -> let c' = singleton c in Put c' c')
        --   $ ['A'..'Z']
        return $ Rs db dir

    destroyDB (Rs db dir) = unsafeClose db `finally` destroy dir defaultOptions
