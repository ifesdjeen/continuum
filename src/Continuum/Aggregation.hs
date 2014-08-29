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
