{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.GenericStorageSpec where

import Continuum.Types
import Continuum.Serialization.Record
import Continuum.Support.QuickCheck
import Continuum.Support.TmpDb
import Continuum.Serialization.Schema  ( makeSchema )
import Continuum.Storage.RecordStorage ( withDecoded )
import GHC.Exts                        ( sortWith )
import Continuum.Storage.GenericStorage
import qualified Continuum.Stream as S

import Test.Hspec
import Test.QuickCheck.Monadic
import Test.QuickCheck

spec :: Spec
spec = do

  describe "Recorg Storage" $ do
    it "Can iterate over the items and append them to the array" $ do
      r <- withTmpDb $ \(Rs db _) -> do
        populate db [ Put "a" "one"
                    , Put "b" "two"
                    , Put "c" "three"]
        res <- withIter db def (\iter -> do
                                   S.toList $ entrySlice iter AllKeys Asc)
        return res
      r `shouldBe` [("a","one"),("b","two"),("c","three")]

    it "Can iterate over the decoded records" $ do
      let schema  = makeSchema [ ("a", DbtLong) ]
          records = [ makeRecord 1 [ ("a", DbLong 1) ]
                    , makeRecord 2 [ ("a", DbLong 2) ]]
      r <- withTmpDb $ \(Rs db _) -> do
        populate db (map (tupleToBatchOp . (encodeRecord schema 1)) records)

        res <- withIter db def (\iter ->
                                 S.collect
                                 $ S.toList
                                 $ withDecoded Record schema
                                 $ entrySlice iter AllKeys Asc)
        return res
      r `shouldBe` (Right records)

    it "Can iterate over larger records" $ do
      let schema  = makeSchema (zip ["a", "b", "c", "d"] [DbtShort, DbtShort, DbtDouble, DbtShort])
          records = [ makeRecord 1 [("c", DbDouble 9.93), ("b", DbShort 2), ("a", DbShort 2), ("d", DbShort 3)]
                    , makeRecord 2 [("c", DbDouble 2.27), ("b", DbShort 3), ("a", DbShort 4), ("d", DbShort 3)]
                    , makeRecord 4 [("c", DbDouble 7.14), ("b", DbShort 4), ("a", DbShort 3), ("d", DbShort 3)]]
      r   <- withTmpDb $ \(Rs db _) -> do
        _   <- populate db (map (tupleToBatchOp . (encodeRecord schema 1)) records)
        res <- withIter db def (\iter -> do
                                   S.collect
                                   $ S.toList
                                   $ withDecoded Record schema
                                   $ entrySlice iter AllKeys Asc)
        return res
      (sortWith ts <$> r) `shouldBe` Right (sortWith ts records)

    it "Can iterate over the decoded records" $ do
      property $ prop_RoundTrip

-- |
-- | Properties
-- |

prop_RoundTrip :: (DbSchema, [DbRecord]) -> Property
prop_RoundTrip (schema, records) = monadicIO $ do
  r   <- run $ withTmpDb $ \(Rs db _) -> do
    _   <- populate db (map (tupleToBatchOp . (encodeRecord schema 1)) records)
    res <- withIter db def (\iter -> S.collect
                                     $ S.toList
                                     $ withDecoded Record schema
                                     $ entrySlice iter AllKeys Asc)
    return res
  assert $ (sortWith ts <$> r) == Right (sortWith ts records)

ts :: DbRecord -> Integer
ts (DbRecord i _) = i
