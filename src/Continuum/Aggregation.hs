module Continuum.Aggregation where

import           Data.ByteString        (ByteString)
import qualified Control.Foldl as L

import           Continuum.Folds
import           Continuum.Storage
import           Continuum.Serialization
import           Continuum.Types


-- aggregateRangeByFields :: Integer
--                           -> Integer
--                           -> [ByteString]
--                           -> L.Fold DbResult acc
--                           -> AppState (Either DbError acc)

-- aggregateRangeByFields rangeBegin rangeEnd fields =
--   scan (TsKeyRange rangeBegin rangeEnd) (Fields fields)

-- aggregateAllByField :: ByteString
--                        -> L.Fold DbResult acc
--                        -> AppState (Either DbError acc)

-- aggregateAllByField field =
--   scan EntireKeyspace (Field field)
