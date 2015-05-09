{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.GenericStorageSpec where

import Continuum.Types
import Continuum.Serialization.Record
import Continuum.Serialization.Schema ( makeSchema )
import Continuum.Support.QuickCheck
import qualified Continuum.Storage.GenericStorage as Storage

import Control.Monad.Catch
import System.Directory
import System.IO.Temp


import Test.Hspec
import Test.QuickCheck.Monadic
import Test.QuickCheck

import Data.Default
import Data.ByteString           ( ByteString )
import Database.LevelDB.Internal (unsafeClose)
import Database.LevelDB.Base     ( BatchOp(..), DB, withIter, write, open, defaultOptions, createIfMissing, destroy )
import GHC.Exts                  ( sortWith )

-- import Debug.Trace

data Rs = Rs DB FilePath

tupleToBatchOp :: (ByteString, ByteString) -> BatchOp
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

    it "asd" $ do
      let schema  = makeSchema (zip ["a", "b", "c", "d"] [DbtShort, DbtShort, DbtDouble, DbtShort])
          records = [ makeRecord 1 [("c", DbDouble 9.93), ("b", DbShort 2), ("a", DbShort 2), ("d", DbShort 3)]
                    , makeRecord 2 [("c", DbDouble 2.27), ("b", DbShort 3), ("a", DbShort 4), ("d", DbShort 3)]
                    , makeRecord 4 [("c", DbDouble 7.14), ("b", DbShort 4), ("a", DbShort 3), ("d", DbShort 3)]]
      r   <- bracket initDB destroyDB $ \(Rs db _) -> do
        _   <- write db def (map (tupleToBatchOp . (encodeRecord schema 1)) records)
        res <- withIter db def (\iter -> do
                                   Storage.toList $ Storage.entrySlice iter AllKeys Asc (decodeRecord Record schema))
        return res
      (sortWith ts <$> r) `shouldBe` Right (sortWith ts records)


    it "Can iterate over the decoded records" $ do
      property $ prop_RoundTrip

-- |
-- | Properties
-- |

prop_RoundTrip :: (DbSchema, [DbRecord]) -> Property
prop_RoundTrip (schema, records) = monadicIO $ do
  r   <- run $ bracket initDB destroyDB $ \(Rs db _) -> do
    _   <- write db def (map (tupleToBatchOp . (encodeRecord schema 1)) records)
    res <- withIter db def (\iter -> do
                               Storage.toList $ Storage.entrySlice iter AllKeys Asc (decodeRecord Record schema))
    return res
  assert $ (sortWith ts <$> r) == Right (sortWith ts records)


initDB :: IO Rs
initDB = do
  tmp <- getTemporaryDirectory
  dir <- createTempDirectory tmp "leveldb-streaming-tests"
  db  <- open dir defaultOptions { createIfMissing = True }
  return $ Rs db dir

destroyDB :: Rs -> IO ()
destroyDB (Rs db dir) = unsafeClose db `finally` destroy dir defaultOptions

ts :: DbRecord -> Integer
ts (DbRecord i _) = i
