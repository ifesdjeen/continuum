{-# LANGUAGE OverloadedStrings #-}

module Continuum.FoldsSpec where

import Continuum.Folds

import Test.Hspec
import Test.QuickCheck

runFold :: (Monoid b) => Fold a b -> [a] -> b
runFold (Fold f z0 e) a = e $ foldl f z0 a

-- prop_MinFold :: (Ord a, Show a) => [a] -> Bool
prop_MinFold :: [Integer] -> Bool
prop_MinFold a@[] = MinNone == runFold op_min a
prop_MinFold a = (Min $ minimum a) == runFold op_min a

prop_CountFold :: [Integer] -> Bool
prop_CountFold a = (Count $ length a ) == runFold op_count a

spec :: Spec
spec = do

  describe "Property Test" $ do
    it "passes Min Fold test" $ do
      property $ prop_MinFold

    it "passes Count Fold test" $ do
      property $ prop_CountFold
