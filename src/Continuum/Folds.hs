{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ExistentialQuantification #-}

module Continuum.Folds where

import Continuum.Types
import Continuum.Serialization.Record
import Data.Maybe       ( fromMaybe )

import qualified Continuum.Stream as S
import Continuum.Stream ( Stream(..), StepError(..), Step(..) )
--import Continuum.Stream  ( Stream(..), StepError(..) )

data (Monoid b) => Fold a b
  -- | @Fold @ @ step @ @ initial @ @ extract@
  = forall x. Fold (x -> a -> x) x (x -> b)

class (Monoid intermediate) => Aggregate intermediate end where
  combine  :: intermediate -> end

fold :: (Monoid b, Monad m) => Fold a b -> Stream m a -> m (Either StepError b)
fold (Fold f z0 fin) stream = fmap fin <$> S.foldl f z0 stream


data Count = Count Int
             deriving (Show, Eq)

instance Monoid Count where
  mempty = Count 0
  mappend (Count a) (Count b) = Count $ a + b

op_count = Fold (\i _ -> i + 1) 0 Count

withField :: Monad m => FieldName -> Stream m DbRecord -> Stream m DbValue
withField f (Stream next0 s0) = Stream next s0
  where
    {-# INLINE next #-}
    next !s = do
        step <- next0 s
        return $ case step of
            StepError e -> StepError e
            Done        -> Done
            Skip    s'  -> Skip        s'
            Yield x s'  -> maybe
                           (StepError NoFieldPresent)
                           (\x' -> Yield x' s')
                           (getValue f x)

data Min i = MinNone
           | Min i
           deriving (Show, Eq)
instance (Ord i) => Monoid (Min i) where
  mempty = MinNone
  mappend (Min i1) (Min i2) = Min $ min i1 i2

op_min :: (Ord i) => Fold i (Min i)
op_min = Fold step MinNone id
  where step MinNone i = Min i
        step m i2 = mappend m (Min i2)
