{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Folds (countFold
                       , appendFold
                       , stopCondition
                       , groupFold)
       where

import Debug.Trace
import qualified Data.Map.Strict as Map
import qualified Control.Foldl as L

-- | Count Fold
countFold :: L.Fold a Int
countFold = L.Fold step 0 id
  where step !acc !i = let res = acc + 1 in (trace (show res) res)

-- | Append Fold
appendFold :: L.Fold a [a]
appendFold = L.Fold step [] id
  where step acc val = acc ++ [val]

-- | Group Fold
groupFold :: (Ord k) =>
              (a -> (k, v))
              -> L.Fold v res
              -> L.Fold a (Map.Map k res)
groupFold conv (L.Fold stepIntern accIntern doneIntern) = L.Fold step Map.empty rewrap
  where step !acc !val =
          let (k,v) = conv val in
          Map.alter (updateFn v) k acc

        rewrap x = Map.map doneIntern x

        -- TODO: this is
        updateFn v i = case i of
          (Just x)  -> Just $ stepIntern x v
          (Nothing) -> Just accIntern

-- | Stop Condition Fold
stopCondition :: (forall acc. (i -> acc -> Bool)) -> L.Fold i done -> L.Fold i done
stopCondition checker (L.Fold step acc done) = L.Fold wrapStep acc done
  where wrapStep acc i = if checker i acc
                      then step acc i
                      else acc



-- L.Fold :: (acc -> i -> acc) -> acc -> (acc -> done) -> L.Fold i done

--- select min value within range
