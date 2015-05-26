{-# LANGUAGE OverloadedStrings #-}

module Continuum.FoldsSpec where

import Continuum.Folds
import Continuum.Types

import Test.Hspec
import Test.QuickCheck
import Data.List ( nubBy )

import qualified Data.Map as Map

-- prop_MinFold :: (Ord a, Show a) => [a] -> Bool
prop_MinFold :: [Integer] -> Bool
prop_MinFold a@[] = MinNone == runFold op_min a
prop_MinFold a = (Min $ minimum a) == runFold op_min a

prop_CountFold :: [Integer] -> Bool
prop_CountFold a = (Count $ length a ) == runFold op_count a

prop_CollectFold :: [Integer] -> Bool
prop_CollectFold a = reverse a == runFold op_collect a

prop_GroupByFold :: [(Integer, [Integer])] -> Bool
prop_GroupByFold a =
  let -- List has to be a non-empty list with unique "key" elements
    a'        = nubBy (\a b -> fst a == fst b) $ filter null a
    expected  = Map.fromList $ fmap (\(k,v) -> (k, runFold op_count v)) a'
    plainList = concat $ fmap (\(k,v) -> fmap ((,) k) v) a'
    result    = runFold (op_groupBy (\i -> Just $ fst i) op_count) plainList
  in expected == result

spec :: Spec
spec = do

  describe "Property Test" $ do
    it "passes Min Fold test" $ do
      property $ prop_MinFold

    it "passes Count Fold test" $ do
      property $ prop_CountFold

    it "passes Collect Fold test" $ do
      property $ prop_CollectFold

    it "passes Group Fold test" $ do
      property $ prop_GroupByFold

  describe "Count" $ do
    it "combines two counts with mappend" $ do
      property $ \a b -> (Count $ a + b) == mappend (Count a) (Count b)
    it "combines two empty with count" $ do
      property $ \a -> Count a == mappend (Count a) mempty

  describe "Min" $ do
    it "combines two mins with mappend" $ do
      property $ ((\a b -> Min (min a b) == mappend (Min a) (Min b)) :: Integer -> Integer -> Bool)
    it "combines two min with mempty" $ do
      property $ ((\a -> Min a == mappend (Min a) MinNone) :: Integer -> Bool)

  describe "Map" $ do
    it "combines two maps with mappend" $ do
      MapResult (Map.fromList [("a",Min 2)
                              ,("b",Min 1)])
      `shouldBe`
      mappend
        (MapResult $ Map.fromList [("a", Min 100)
                                 , ("b", Min 1)])
        (MapResult $ Map.fromList [("a", Min 2)
                                  , ("b", Min 200)])
