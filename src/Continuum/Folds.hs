{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ExistentialQuantification #-}

module Continuum.Folds where

import Continuum.Types

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

-- | Runs the fold
runFold :: (Monoid b) => Fold a b -> [a] -> b
runFold (Fold f z0 e) a = e $ foldl f z0 a
