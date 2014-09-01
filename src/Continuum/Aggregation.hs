module Continuum.Aggregation where

import           Data.ByteString        (ByteString)
import qualified Control.Foldl as L

import           Continuum.Folds
import           Continuum.Storage
import           Continuum.Serialization
import           Continuum.Types


aggregateRangeByFields :: Integer
                          -> Integer
                          -> [ByteString]
                          -> L.Fold (Integer, [DbValue]) acc
                          -> AppState (Either DbError acc)

aggregateRangeByFields rangeBegin rangeEnd fields foldOp =
  gaplessScan (Just begin) (decodeFieldsByName2 fields) (stopCondition checker foldOp)
  where begin = encodeBeginTimestamp rangeBegin
        checker = matchTs (<=) rangeEnd


aggregateRangeByField :: Integer
                         -> Integer
                         -> ByteString
                         -> ((Integer, DbValue) -> i)
                         -> L.Fold i acc
                         -> AppState (Either DbError acc)

aggregateRangeByField rangeBegin rangeEnd field mapFn (L.Fold reduceFn acc done) =
  scan (Just begin) (withField field mapFn checker reduceFn) acc done
  where begin = encodeBeginTimestamp rangeBegin
        checker = matchTs (<=) rangeEnd


aggregateAllByField :: (Show acc, Eq acc) =>
                       ByteString
                       -> ((Integer, DbValue) -> i)
                       -> L.Fold i acc
                       -> AppState (Either DbError acc)

aggregateAllByField field mapFn (L.Fold reduceFn acc done) =
  scan Nothing (withField field mapFn alwaysTrue reduceFn) acc done

aggregateAllByRecord :: (DbRecord -> i)
                        -> L.Fold i acc
                        -> AppState (Either DbError acc)

aggregateAllByRecord mapFn (L.Fold reduceFn acc done) =
  scan Nothing (withFullRecord mapFn alwaysTrue reduceFn) acc done
