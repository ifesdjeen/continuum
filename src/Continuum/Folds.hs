{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE Rank2Types #-}

module Continuum.Folds (countFold, appendFold, stopCondition)
       where

import qualified Control.Foldl as L

-- data Fold a b = forall x . Fold (x -> a -> x) x (x -> b)

countFold :: L.Fold Int Int
countFold = L.Fold step 0 id
            where step acc i = acc + 1

appendFold :: L.Fold a [a]
appendFold = L.Fold step [] id
            where step acc val = acc ++ [val]

stopCondition :: (forall acc. (i -> acc -> Bool)) -> L.Fold i done -> L.Fold i done
stopCondition checker (L.Fold step acc done) = L.Fold step2 acc done
  where step2 acc i = if checker i acc
                      then step acc i
                      else acc

-- L.Fold :: (acc -> i -> acc) -> acc -> (acc -> done) -> L.Fold i done

--- select min value within range
