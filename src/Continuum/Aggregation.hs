module Continuum.Aggregation where

import           Data.ByteString        (ByteString)

import Continuum.Storage
import Continuum.Serialization
import Continuum.Types

aggregateRangeByFields :: (Show acc, Eq acc) =>
                          Integer
                          -> Integer
                          -> [ByteString]
                          -> ((Integer, [DbValue]) -> i)
                          -> (i -> acc -> acc)
                          -> acc
                          -> AppState (Either DbError acc)

aggregateRangeByFields rangeBegin rangeEnd fields mapFn reduceFn acc =
  scan (Just begin) (withFields fields mapFn checker reduceFn) acc
  where begin = encodeBeginTimestamp rangeBegin
        checker = matchTs (<=) rangeEnd


aggregateRangeByField :: (Show acc, Eq acc) =>
                         Integer
                         -> Integer
                         -> ByteString
                         -> ((Integer, DbValue) -> i)
                         -> (i -> acc -> acc)
                         -> acc
                         -> AppState (Either DbError acc)

aggregateRangeByField rangeBegin rangeEnd field mapFn reduceFn acc =
  scan (Just begin) (withField field mapFn checker reduceFn) acc
  where begin = encodeBeginTimestamp rangeBegin
        checker = matchTs (<=) rangeEnd


aggregateAllByField :: (Show acc, Eq acc) =>
                       ByteString
                       -> ((Integer, DbValue) -> i)
                       -> (i -> acc -> acc)
                       -> acc
                       -> AppState (Either DbError acc)

aggregateAllByField field mapFn reduceFn acc =
  scan Nothing (withField field mapFn alwaysTrue reduceFn) acc

aggregateAllByRecord :: (Show acc, Eq acc) =>
                        (DbRecord -> i)
                        -> (i -> acc -> acc)
                        -> acc
                        -> AppState (Either DbError acc)

aggregateAllByRecord mapFn reduceFn acc =
  scan Nothing (withFullRecord mapFn alwaysTrue reduceFn) acc


--- select min value within range
