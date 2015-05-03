{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.RecordStorageSpec where

import Continuum.Storage.RecordStorage
import           Control.Applicative        hiding (empty)
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

spec :: Spec
spec = do

  describe "Primitive" $ do
    it "passes Word8  Round Trip" $ do
      r <- bracket initDB destroyDB $ \(Rs db _) -> do
        write db def [ Put "a" "one"
                     , Put "b" "two"
                     , Put "c" "three"]
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
