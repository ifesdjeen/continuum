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
import GHC.Exts ( sortWith )

-- import Debug.Trace

data Rs = Rs DB FilePath

tupleToBatchOp :: (ByteString, ByteString) -> BatchOp
tupleToBatchOp (x,y) = Put x y

prop_RoundTrip :: (DbSchema, [DbRecord]) -> Property
prop_RoundTrip (schema, records) = monadicIO $ do
  r   <- run $ bracket initDB destroyDB $ \(Rs db _) -> do
    _   <- write db def (map (tupleToBatchOp . (encodeRecord schema 1)) records)
    res <- withIter db def (\iter -> do
                               Storage.toList $ Storage.entrySlice iter AllKeys Asc (decodeRecord Record schema))
    return res
  assert $ (sortWith ts <$> r) == Right (sortWith ts records)

  where ts (DbRecord i _) = i


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

--    it "asd" $ do

      -- let schema  = makeSchema (zip ["\184","mJs","\ENQ;'","\222S\DLEf","\ENQ\129\137\DC3"] [DbtShort,DbtShort,DbtDouble,DbtShort,DbtFloat])
      --     records = [makeRecord 1 ([("\ENQ;'",DbDouble 9.931976876439089),("\ENQ\129\137\DC3",DbFloat 14.999481),("mJs",DbShort 2),("\184",DbShort 2),("\222S\DLEf",DbShort 3)]),makeRecord 2 ([("\ENQ;'",DbDouble 2.277007862297778),("\ENQ\129\137\DC3",DbFloat 0.98638123),("mJs",DbShort 2),("\184",DbShort 4),("\222S\DLEf",DbShort 3)]),makeRecord 4 ([("\ENQ;'",DbDouble 7.144124519534123e-4),("\ENQ\129\137\DC3",DbFloat 2.1302373),("mJs",DbShort 2),("\184",DbShort 3),("\222S\DLEf",DbShort 3)]),makeRecord 1 ([("\ENQ;'",DbDouble 3.9215858195166775),("\ENQ\129\137\DC3",DbFloat 7.664713),("mJs",DbShort 1),("\184",DbShort 3),("\222S\DLEf",DbShort 5)])]
      -- r   <- bracket initDB destroyDB $ \(Rs db _) -> do
      --   _   <- write db def (map (tupleToBatchOp . (encodeRecord schema 1)) records)
      --   res <- withIter db def (\iter -> do
      --                              Storage.toList $ Storage.entrySlice iter AllKeys Asc (decodeRecord Record schema))
      --   return res
      -- (sortWith ts <$> r) `shouldBe` Right (sortWith ts records)
      -- where ts (DbRecord i _) = i

    it "Can iterate over the decoded records" $ do
      property $ prop_RoundTrip


initDB :: IO Rs
initDB = do
  tmp <- getTemporaryDirectory
  dir <- createTempDirectory tmp "leveldb-streaming-tests"
  db  <- open dir defaultOptions { createIfMissing = True }
  return $ Rs db dir

destroyDB :: Rs -> IO ()
destroyDB (Rs db dir) = unsafeClose db `finally` destroy dir defaultOptions
