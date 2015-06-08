{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.RecordStorageSpec where

import Continuum.Types
import Continuum.Serialization.Record
import Continuum.Support.QuickCheck
import Continuum.Support.TmpDb
import GHC.Exts                        ( sortWith )
import Continuum.Folds
import Continuum.Storage.RecordStorage

import Test.Hspec
import Test.QuickCheck.Monadic
import Test.QuickCheck

spec :: Spec
spec = do

  describe "Recorg Storage" $ do
    it "Runs min query" $ do
      let schema  = makeSchema [ ("a", DbtLong) ]
          records = [ makeRecord 1 [ ("a", DbLong 5) ]
                    , makeRecord 2 [ ("a", DbLong 10) ]
                    , makeRecord 3 [ ("a", DbLong 15) ]
                    , makeRecord 4 [ ("a", DbLong 2) ]
                    , makeRecord 5 [ ("a", DbLong 30) ]]
      r <- withTmpDb $ \(Rs db _) -> do
        populate db (map (tupleToBatchOp . (encodeRecord schema 1)) records)
        fieldQuery db AllKeys Record schema (op_withField "a" op_min)
      r `shouldBe` (Min $ DbLong 2)

    it "Runs min query 2" $ do
      let schema  = makeSchema (zip ["a", "b", "c", "d"] [DbtShort, DbtShort, DbtDouble, DbtShort])
          records = [ makeRecord 1 [("c", DbDouble 9.93), ("b", DbShort 2), ("a", DbShort 5), ("d", DbShort 3)]
                    , makeRecord 2 [("c", DbDouble 2.27), ("b", DbShort 3), ("a", DbShort 4), ("d", DbShort 3)]
                    , makeRecord 4 [("c", DbDouble 7.14), ("b", DbShort 4), ("a", DbShort 2), ("d", DbShort 3)]]
      r   <- withTmpDb $ \(Rs db _) -> do
        _   <- populate db (map (tupleToBatchOp . (encodeRecord schema 1)) records)
        fieldQuery db AllKeys Record schema (op_withField "a" op_min)
      r `shouldBe` (Min $ DbShort 2)

    it "Will run a collect fold" $ do
      property $ prop_CollectFold


prop_CollectFold :: (DbSchema, [DbRecord]) -> Property
prop_CollectFold (schema, records) = monadicIO $ do
  r   <- run $ withTmpDb $ \(Rs db _) -> do
    _   <- populate db (map (tupleToBatchOp . (encodeRecord schema 1)) records)
    fieldQuery db AllKeys Record schema op_collect
  assert $ (sortWith ts r) == (sortWith ts records)

ts :: DbRecord -> Integer
ts (DbRecord i _) = i
