{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Folds (countFold
                       , appendFold
                       , groupFold)
       where

import Continuum.Types
import Debug.Trace
import qualified Data.Map.Strict as Map
import qualified Control.Foldl as L

-- forall .acc br Fold (acc -> i -> acc) i (acc -> done)
-- Fold i done
-- | Count Fold
countFold :: L.Fold a Int
countFold = L.Fold step 0 id
  where step acc i = acc + 1

-- | Append Fold
appendFold :: L.Fold a [a]
appendFold = L.Fold step [] id
  where step acc val = acc ++ [val]

groupFold :: (Ord k) =>
              (DbResult -> (k, v))
              -> L.Fold v res
              -> L.Fold DbResult (Map.Map k res)
groupFold conv (L.Fold stepIntern accIntern doneIntern) = L.Fold step Map.empty rewrap
  where
    {-# INLINE step #-}
    step !acc !val =
      let (k,v) = conv val in
      Map.alter (updateFn v) k acc

    rewrap x = Map.map doneIntern x

    {-# INLINE updateFn #-}
    updateFn v i = case i of
      (Just x)  -> Just $! stepIntern x v
      (Nothing) -> Just accIntern
