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
  gaplessScan (Just begin) (decodeFieldsByName fields) (stopCondition checker foldOp)
  where begin = encodeBeginTimestamp rangeBegin
        checker = matchTs (<=) rangeEnd


aggregateRangeByField :: Integer
                         -> Integer
                         -> ByteString
                         -> L.Fold (Integer, DbValue) acc
                         -> AppState (Either DbError acc)

aggregateRangeByField rangeBegin rangeEnd field foldOp =
  gaplessScan (Just begin) (decodeFieldByName field) (stopCondition checker foldOp)
  where begin = encodeBeginTimestamp rangeBegin
        checker = matchTs (<=) rangeEnd


aggregateAllByField :: ByteString
                       -> L.Fold (Integer, DbValue) acc
                       -> AppState (Either DbError acc)

aggregateAllByField field =
  gaplessScan Nothing (decodeFieldByName field)

aggregateAllByRecord :: (DbRecord -> i)
                        -> L.Fold DbRecord acc
                        -> AppState (Either DbError acc)

aggregateAllByRecord mapFn =
  gaplessScan Nothing decodeRecord
