{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}

module Continuum.Folds where

import Continuum.Types
import Continuum.Serialization.Record ( getValue )

import qualified Data.Map.Strict as Map
import qualified Data.Stream.Monadic as M
-- import Database.LevelDB.Streaming ( KeyRange(..) )

data (Monad m, Monoid b) => FoldM a m b
  = forall x. FoldM (x -> a -> m x) x (x -> m b)

data (Monoid b) => Fold a b
  -- | @Fold @ @ step @ @ initial @ @ extract@
  = forall x. Fold (x -> a -> x) x (x -> b)

class (Monoid intermediate) => Aggregate intermediate end where
  combine  :: intermediate -> end

fold :: (Monoid b, Monad m) => Fold a b -> M.Stream m a -> m b
fold (Fold f z0 fin) stream = fmap fin $ M.foldl f z0 stream

data Count = Count Int
             deriving (Show, Eq)

instance Monoid Count where
  mempty = Count 0
  mappend (Count a) (Count b) = Count $ a + b

instance Aggregate Count Int where
  combine (Count a) = a

op_count :: forall a. Fold a Count
op_count = Fold (\i _ -> i + 1) 0 Count

data (Ord i) => Min i = MinNone
              | Min i
           deriving (Show, Eq)

instance (Ord i) => Monoid (Min i) where
  mempty                    = MinNone
  mappend (Min i1) (Min i2) = Min $ min i1 i2
  mappend x        MinNone  = x
  mappend MinNone  x        = x

op_min :: (Ord i) => Fold i (Min i)
op_min = Fold step MinNone id
  where step MinNone i = Min i
        step m i2 = mappend m (Min i2)

instance (Ord i) => Aggregate (Min i) i where
  combine (Min a) = a

op_collect :: forall a. Fold a [a]
op_collect = Fold (flip (:)) [] id

-- | Runs the fold
runFold :: (Monoid b) => Fold a b -> [a] -> b
runFold (Fold f z0 e) a = e $ foldl f z0 a

op_withField :: (Monoid b) => FieldName -> (Fold DbValue b) -> Fold DbRecord b
op_withField fieldName (Fold f z0 e) =
  let valueFn      = getValue fieldName
      wrapped z1 x = maybe z1 (f z1) (valueFn x)
  in
   Fold wrapped z0 e

op_groupBy :: (Ord a, Monoid b) => (r -> Maybe a) -> (Fold r b) -> Fold r (MapResult a b)
op_groupBy groupFn (Fold f z0 e) =
  let subStep n Nothing  = return $! (f z0 n)
      subStep n (Just a) = return $! (f a n)
      localStep m record = maybe m (\r -> Map.alter (subStep record) r m) (groupFn record)
      done a             = MapResult $ Map.map e a
  in Fold localStep Map.empty done

op_groupByField :: (Monoid b) => FieldName -> (Fold DbRecord b) -> Fold DbRecord (MapResult DbValue b)
op_groupByField fieldName f = op_groupBy (getValue fieldName) f

data (Integral a) => Mean a = Mean [a]
            deriving (Show, Eq)

op_mean :: (Integral a) => Fold a (Mean a)
op_mean = Fold (flip (:)) [] Mean

instance (Integral a) => Monoid (Mean a) where
  mempty = Mean []
  mappend (Mean a) (Mean b) = Mean $ a ++ b

instance (Integral a) => Aggregate (Mean a) Double where
  combine (Mean []) = 0
  combine (Mean a) = s / l
    where s = fromIntegral $ sum a
          l = fromIntegral $ length a

-- class (Monoid intermediate) => Aggregate intermediate end where
--   combine  :: intermediate -> end


-- data Median = Mean [Number]
--           deriving (Show, Eq)
