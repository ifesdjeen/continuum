module Continuum.Storage.ChunkStorage where

import Continuum.Types
import Database.LevelDB.Base
import Continuum.Serialization.Primitive ( packWord64 )

toKeyRange :: TimeRange -> KeyRange
toKeyRange AllTime = AllKeys
toKeyRange TimeBetween s e =
  let endBytes = packWord64 e
      KeyRange {start = packWord64 s,
                end = (\x -> x `compare` s)}
