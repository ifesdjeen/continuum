{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.RecordStorageSpec where

import Continuum.Serialization.Record
import Continuum.Serialization.Schema ( makeSchema )
import qualified Continuum.Storage.RecordStorage as Storage
import Continuum.Types
import           Control.Monad.Catch
import           Database.LevelDB.Base
import           Database.LevelDB.Internal  (unsafeClose)
import           Data.ByteString                   ( ByteString )
import           System.Directory
import           System.IO.Temp
import           Data.Default
import Test.Hspec

import Continuum.Support.QuickCheck

-- import Debug.Trace

data Rs = Rs DB FilePath

tupleToBatchOp :: (ByteString, ByteString) -> BatchOp
tupleToBatchOp (x,y) = Put x y

roundTrip :: [SchemaTestRow] -> Bool
roundTrip testRows =
  let schema  = makeSchema $ fmap (\(TestRow name tp _) -> (name, tp)) testRows
      record  = makeRecord 123 $ fmap (\(TestRow name _ vl) -> (name, vl)) testRows
      encoded = encodeRecord schema 123 record
      decoder = decodeRecord Record schema
  in
   (Right record) == (decoder encoded)


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
