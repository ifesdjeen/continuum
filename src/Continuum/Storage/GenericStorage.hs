{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE ExistentialQuantification #-}
module Continuum.Storage.GenericStorage where

import Control.Applicative
import Continuum.Types

import Continuum.Stream
import Control.Monad.IO.Class
import Database.LevelDB.Base

import Data.Either     ( either )
import Control.Monad   ( Monad (..), (=<<) )

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
