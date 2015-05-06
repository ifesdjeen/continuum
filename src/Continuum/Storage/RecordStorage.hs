{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE ExistentialQuantification #-}
module Continuum.Storage.RecordStorage where

import Control.Applicative
import Continuum.Types

import Control.Monad.IO.Class
import Database.LevelDB.Base

-- import Data.ByteString ( ByteString )
import Data.Either     ( either )
import Control.Monad   ( Monad (..), (=<<) )

import Prelude (Either (..), Functor (..),
                Maybe (..), Ord (..), Ordering (..),
                otherwise, ($), (.) )

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

entrySlice :: (Applicative m, MonadIO m)
           => Iterator
           -> KeyRange
           -> Direction
           -> Decoder a
           -> Stream m a -- Entry

entrySlice i (KeyRange s e) direction decoder = Stream next (iterSeek i s >> pure i)
  where
    next it = do
        entry <- iterEntry it
        case entry of
         Nothing       -> pure Done
         Just x@(!k,_) -> either
                          (\_       -> pure $ StepError EmptyStepError)
                          (\decoded -> case direction of
                                        Asc  | e k < GT  -> Yield decoded <$> (iterNext it >> pure it)
                                             | otherwise -> pure Done
                                        Desc | e k > LT  -> Yield decoded <$> (iterPrev it >> pure it)
                                             | otherwise -> pure Done)
                          (decoder x)

entrySlice i AllKeys Asc decoder = Stream next (iterFirst i >> pure i)
  where
    next it = do
      entry <- iterEntry it
      case entry of
       Nothing -> pure Done
       Just x  -> either
                  (\_       -> pure $ StepError EmptyStepError)
                  (\decoded -> Yield decoded <$> (iterNext it >> pure it))
                  (decoder x)

entrySlice i AllKeys Desc decoder = Stream next (iterLast i >> pure i)
  where
    next it = do
      entry <- iterEntry it
      case entry of
       Nothing -> pure Done
       Just x  -> either
                  (\_       -> pure $ StepError EmptyStepError)
                  (\decoded -> Yield decoded <$> (iterPrev it >> pure it))
                  (decoder x)

-- putRecord :: DbName
--              -> DbRecord
--              -> DbState DbResult
-- putRecord dbName record = do
--   sid          <- getAndincrementSequence
--   _            <- maybeWriteChunk sid record
--   maybeDbDescr <- getDb dbName

--   case maybeDbDescr of
--     (Just (schema, db)) -> do
--       wo <- getWriteOptions
--       when (validateRecord record schema) $ do
--         let (key, value) = encodeRecord schema record sid
--         _  <- LDB.put db wo key value
--         return ()
--       return $ return $ EmptyRes
--     Nothing             -> return $ Left NoSuchDatabaseError
