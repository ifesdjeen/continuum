{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE ExistentialQuantification #-}
module Continuum.Stream where

import Control.Monad   ( Monad (..), (=<<) )

import Prelude (Either (..), Functor (..), Eq(..),
                Show(..), ($), (.), (<$>) )

-- | Iteration Error
data StepError = EmptyStepError
               | NoFieldPresent
                 deriving(Eq, Show)

-- | Streaming Types
data Step   a  s
   = Yield  a !s
   | Skip  !s
   | StepError StepError
   | Done

-- | Stream
data Stream m a = forall s. Stream (s -> m (Step a s)) (m s)

instance Monad m => Functor (Stream m) where
    fmap = map

map :: Monad m => (a -> b) -> Stream m a -> Stream m b
map f (Stream next0 s0) = Stream next s0
  where
    {-# INLINE next #-}
    next !s = do
        step <- next0 s
        return $ case step of
            StepError e -> StepError e
            Done        -> Done
            Skip    s'  -> Skip        s'
            Yield x s'  -> Yield (f x) s'
{-# INLINE [0] map #-}
{-# RULES
"Stream map/map fusion" forall f g s.
    map f (map g s) = map (f . g) s
  #-}

foldl :: Monad m => (b -> a -> b) -> b -> Stream m a -> m (Either StepError b)
foldl f z0 (Stream next s0) = loop z0 =<< s0
  where
    loop z !s = do
        step <- next s
        case step of
         StepError e -> return $ Left e
         Done        -> return $ Right z
         Skip    s'  -> loop z s'
         Yield x s'  -> loop (f z x) s'
{-# INLINE [0] foldl #-}
{-# RULES
"Stream foldl/map fusion" forall f g z s.
    foldl f z (map g s)  = foldl (\ z' -> f z' . g) z s

  #-}

fromList :: Monad m => [a] -> Stream m a
fromList xs = Stream next (return xs)
  where
    {-# INLINE next #-}
    next []      = return Done
    next (x:xs') = return $ Yield x xs'
{-# INLINE [0] fromList #-}

toList :: (Functor m, Monad m) => Stream m a -> m (Either StepError [a])
toList (Stream next s0) = unfold =<< s0
  where
    unfold !s = do
        step <- next s
        case step of
         StepError e -> return $ Left e
         Done        -> return $ Right []
         Skip    s'  -> unfold s'
         Yield x s'  -> (\y -> (x :) <$> y) <$> unfold s'
{-# INLINE [0] toList #-}
