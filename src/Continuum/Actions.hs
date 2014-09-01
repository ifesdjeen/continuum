{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}

module Continuum.Actions where

import           Data.Maybe (fromJust, maybe)
import qualified Data.Map as Map
import           Data.List (nub)
import qualified Data.Set as Set
import           Data.ByteString        (ByteString)

import Continuum.Serialization
import Control.Applicative

extractField :: ByteString -> DbRecord -> DbValue
extractField field (DbRecord _ record) = fromJust $ Map.lookup field record

groupBy :: (Ord a) => [b] -> (b -> a) -> (Group a b)
groupBy records groupFn = Group $ Map.toList $ foldr appendToGroup Map.empty records
                          where appendToGroup entry acc = Map.insertWith (++) (groupFn entry) [entry] acc

gradualGroupBy :: (Ord a) => a -> Map.Map a Integer -> Map.Map a Integer
gradualGroupBy i acc = Map.insertWith (+) i 1 acc


---- Ord k => (Maybe a -> Maybe a) -> k -> Map.Map k a -> Map.Map k a

groupReduce :: (Ord a) =>
               (i -> a)
               -> (i -> b)
               -> (b -> acc -> acc)
               -> acc
               -> i
               -> Map.Map a acc
               -> Map.Map a acc

groupReduce keyFn valueFn foldFn accInit entry m = --undefined
  Map.alter updateFn (keyFn entry) m
  where updateFn i = case i of
          (Just x) -> Just $ foldFn (valueFn entry) x
          (Nothing) -> Just accInit



-- class FoldOperation a acc where
--   initial :: a
--   foldOp :: (a -> acc -> acc)


  -- foldOp i acc = i + acc
  -- doFold Sum
-- instance Foldable (SumFold) where
--   foldl





-- groupCount :: (Ord a) => a -> Map.Map a Integer -> Map.Map a Integer
-- groupCount entry acc =

-- groupBy :: (Ord a) => [DbRecord] -> (DbRecord -> a) -> (Group a DbRecord)
-- groupBy records groupFn = Group $ Map.toList $ foldr appendToGroup Map.empty records
--                           where appendToGroup entry acc = Map.insertWith (++) (groupFn entry) [entry] acc

test = let records = [ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))],
                       makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "1"))],
                       makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "2"))],
                       makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))],
                       makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "1"))],
                       makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "2"))] ]
       in
   foldGroup1 (+) $ unpackInt <$> (extractField "a") <$> groupBy records (byField "b")

-- foldl1 (+) (fmap unpackInt [DbInt 1, DbInt 1])
-- foldl1 min [DbInt 1, DbInt 1]
-- foldl1 max [DbInt 1, DbInt 1]

-- let records = [ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))], makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "2"))], makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "3"))], makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))], makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))], makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))] ]
-- fmap (extractField "a") records

-- foldl1 (+) $ unpackInt <$> (extractField "a") <$> records
