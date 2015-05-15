{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.ChunkStorage where

import Continuum.Types
import Continuum.Serialization.Primitive ( packWord64, unpackWord64 )
import Continuum.Storage.GenericStorage
import qualified Continuum.Stream as S

import Data.Default
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch    (MonadMask)
import Database.LevelDB.Iterator ( withIter )

fetchChunks :: (MonadMask m, MonadIO m) => TimeRange -> DB -> m [KeyRange]
fetchChunks range db = withIter db def f
  where f iter = do
          fmap (addBounds range) $ S.toList $ keySlice iter (toKeyRange range) Asc

(<<) :: (Ord a) => a -> a -> Ordering
(<<) a b
  | a < b = LT
  | otherwise = GT


addBounds :: TimeRange -> [DbKey] -> [KeyRange]
addBounds a@AllTime (h1:h2:t) = KeyRange {start = h1,
                                          end   = (\x -> x << h2)} : (addBounds a ([h2] ++ t))
addBounds a@AllTime [h1]      = [KeyRange {start = h1,
                                           end   = (\_ -> LT)}]
addBounds a@AllTime []        = [AllKeys]

addBounds (TimeBetween b e) x =
  let startBytes = packWord64 b
      endBytes   = packWord64 e
      ranges     = [startBytes] ++ x ++ [endBytes]
  in toKeyRanges ranges
  where makeRange h1 h2        = KeyRange {start = h1, end = (\x -> x << h2)}
        toKeyRanges []         = []
        toKeyRanges [h1, h2]   = [(makeRange h1 h2)]
        toKeyRanges (h1:h2:xs) = (makeRange h1 h2) : (toKeyRanges ([h2] ++ xs))

encodeChunkKey :: Integer -> ChunkKey
encodeChunkKey = packWord64
{-# INLINE encodeChunkKey #-}

decodeChunkKey :: Decoder Integer
decodeChunkKey (x, _) = unpackWord64 x
{-# INLINE decodeChunkKey #-}

toKeyRange :: TimeRange -> KeyRange
toKeyRange AllTime = AllKeys
toKeyRange (TimeBetween s e) =
  let endBytes = packWord64 e
  in KeyRange {start = packWord64 s,
               end = (\x -> x `compare` endBytes)}
