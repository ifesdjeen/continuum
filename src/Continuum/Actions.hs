module Continuum.Actions where

import           Data.Maybe (fromJust)
import qualified Data.Map as Map
import           Data.List (nub)
import qualified Data.Set as Set

import Continuum.Serialization
import Control.Applicative

extractField :: String -> DbRecord -> DbValue
extractField field (DbRecord _ record) = fromJust $ Map.lookup field record

groupBy :: (Ord a) => [b] -> (b -> a) -> (Group a b)
groupBy records groupFn = Group $ Map.toList $ foldr appendToGroup Map.empty records
                          where appendToGroup entry acc = Map.insertWith (++) (groupFn entry) [entry] acc


gradualGroupBy :: (Ord a) => a -> Map.Map a Integer -> Map.Map a Integer
gradualGroupBy i acc = Map.insertWith (+) i 1 acc


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
