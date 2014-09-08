{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord, findByTimestamp, findRange, gaplessScan,
        alwaysTrue)
       where

import           Debug.Trace
import Control.Applicative
import           Continuum.Types
import           Continuum.Folds
import           Continuum.Options
import           Continuum.Serialization
import qualified Database.LevelDB.MonadResource  as Base
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterSeek, iterFirst, -- iterItems,
                                                 withIterator, iterNext, iterEntry)
import           Data.ByteString        (ByteString)
import           Control.Monad.State.Strict
import           Data.Maybe (isJust, fromJust)
import           Control.Monad.Trans.Resource
import qualified Control.Foldl as L


-- type AppResource a = ResourceT AppState IO a


makeContext :: DB -> DbSchema -> RWOptions -> DBContext
makeContext db' schema' rwOptions' = DBContext {ctxDb = db',
                                                sequenceNumber = 1,
                                                ctxSchema = schema',
                                                ctxRwOptions = rwOptions'}

db :: MonadState DBContext m => m DB
db = gets ctxDb

getAndincrementSequence :: MonadState DBContext m => m Integer
getAndincrementSequence = do
  old <- get
  -- put old {sequenceNumber = (sequenceNumber old) + 1}
  modify (\a -> a {sequenceNumber = (sequenceNumber a) + 1})
  return $ sequenceNumber old

schema :: MonadState DBContext m => m DbSchema
schema = gets ctxSchema

rwOptions :: MonadState DBContext m => m RWOptions
rwOptions = gets ctxRwOptions

ro :: MonadState DBContext m => m ReadOptions
ro = liftM fst rwOptions

wo :: MonadState DBContext m => m WriteOptions
wo = liftM snd rwOptions

storagePut :: (ByteString, ByteString) -> AppState ()
storagePut (key, value) = do
  db' <- db
  wo' <- wo
  Base.put db' wo' key value

putRecord :: DbRecord -> AppState ()
putRecord record = do
  sid <- getAndincrementSequence
  schema' <- schema
  storagePut $ encodeRecord schema' record sid

-- | Find a particular record by the timestamp
findByTimestamp :: Integer -> AppState (Either DbError [DbRecord])
findByTimestamp timestamp = gaplessScan (Just begin) decodeRecord (stopCondition checker appendFold)
                            where begin   = encodeBeginTimestamp timestamp
                                  checker = matchTs (==) timestamp


-- | Find records in the range
findRange :: Integer -> Integer -> AppState (Either DbError [DbRecord])
findRange beginTs end = gaplessScan (Just begin) decodeRecord (stopCondition checker appendFold)
                      where begin = encodeBeginTimestamp beginTs
                            checker = matchTs (<=) end


alwaysTrue :: a -> b -> Bool
alwaysTrue = \_ _ -> True

runApp :: String -> DbSchema -> AppState a -> IO (a)
runApp path schema' actions = do
  runResourceT $ do
    -- TODO: add indexes
    db' <- Base.open path opts
    let ctx = makeContext db' schema' (readOpts, writeOpts)
    -- liftResourceT $ (flip evalStateT) ctx actions
    res <- (flip evalStateT) ctx actions
    return $ res

-- compareKeys :: ByteString -> ByteString -> Ordering
-- compareKeys k1 k2 = compare k11 k21
--                     where (k11, k12) = decodeKey k1
--                           (k21, k22) = decodeKey k2
-- customComparator :: Comparator
-- customComparator = Comparator compareKeys


-- StateT DBContext (ResourceT IO) a
-- type ScanOp a = ResourceT (Either String) a

forceM :: Monad m => m a -> m a
forceM m = do v <- m; return $! v

strictFmap :: Monad m => (a -> b) -> m a -> m b
strictFmap f m = liftM f (forceM m)

gaplessScan ::  Maybe ByteString
         -> (DbSchema -> (ByteString, ByteString) -> (Either DbError i))
         -> L.Fold i acc
         -> AppState (Either DbError acc)

gaplessScan begin mapop (L.Fold foldop acc done) = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIterator db' ro' $ \iter -> do
               if (isJust begin)
                 then iterSeek iter (fromJust begin)
                 else iterFirst iter
               -- let step acc x = (mapop schema' x) >>= (\y -> return $ foldop acc' y)
               --- acc is either
               --- result of mapop is either
               let step !acc !x = (mapop schema' x) >>= \ !i -> strictFmap (\ !acc' -> foldop acc' i) acc
               scanIntern iter step (Right acc)
  return $! fmap done records -- (records >>= (\x -> return $ done x))

-- scanIntern  :: (MonadResource m) =>
--                Iterator
--                -> (acc -> (ByteString, ByteString) -> acc)
--                -> acc
--                -> m acc

-- scanIntern iter op acc = s acc
--   where s !acc = do
--           next <- iterEntry iter
--           iterNext iter

--           if isJust next
--             then s (op acc (fromJust next))
--             else return acc


scanIntern :: (MonadResource m) =>
               Iterator
               -> (acc -> (ByteString, ByteString) -> acc)
               -> acc
               -> m acc

scanIntern iter op orig = s orig
  where s !acc = do
          next <- iterEntry iter
          iterNext iter

          if isJust next
            then s (op acc (fromJust next))
            -- a <- s
            -- return $ (op a (fromJust next))

            else return acc


-- TODO: add batch put operstiaon
-- TODO: add delete operation
