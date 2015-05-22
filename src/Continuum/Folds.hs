{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ExistentialQuantification #-}

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

op_count :: forall a. Fold a Count
op_count = Fold (\i _ -> i + 1) 0 Count

data Min i = MinNone
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

op_collect :: forall a. Fold a [a]
op_collect = Fold (flip (:)) [] id

-- | Runs the fold
runFold :: (Monoid b) => Fold a b -> [a] -> b
runFold (Fold f z0 e) a = e $ foldl f z0 a

op_withField :: (Monoid b) => FieldName -> (Fold DbValue b) -> Fold DbRecord (Maybe b)
op_withField fieldName (Fold f z0 e) =
  let valueFn      = getValue fieldName
      wrapped z1 x = f <$> z1 <*> (valueFn x)
  in
   Fold wrapped (Just z0) (fmap e)

op_groupByField :: (Monoid b) => FieldName -> (Fold DbRecord b) -> Fold DbRecord (Map.Map (Maybe DbValue) b)
op_groupByField fieldName (Fold f z0 e) =
  let valueFn                   = getValue fieldName
      wrappedSubStep n Nothing  = return $! (f z0 n) -- is <$>
      wrappedSubStep n (Just a) = return $! (f a n)
      localStep m record        = Map.alter (wrappedSubStep record) (valueFn record) m
      done                      = Map.map e
  in Fold localStep Map.empty done
