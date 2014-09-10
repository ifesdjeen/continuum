{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord, findByTimestamp, findRange, gaplessScan,
        alwaysTrue)
       where

-- import           Debug.Trace
import           Continuum.Types
import           Continuum.Folds
import           Continuum.Options
import           Continuum.Serialization
import qualified Database.LevelDB.MonadResource  as Base
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterSeek, iterFirst, -- iterItems,
                                                 withIterator, iterNext, iterEntry)
import           Data.ByteString                (ByteString)
import           Control.Monad.State.Strict
import           Data.Maybe                     (isJust, fromJust)
import           Control.Monad.Trans.Resource
import qualified Control.Foldl as L


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

liftEither :: (a -> b -> c) -> Either DbError a -> Either DbError b -> Either DbError c
liftEither f (Right a) (Right b) = return $! f a b
liftEither _ (Left a)  _         = Left a
liftEither _ _         (Left b)  = Left b

gaplessScan :: Maybe ByteString
            -> (DbSchema -> (ByteString, ByteString) -> (Either DbError i))
            -> L.Fold i acc
            -> AppState (Either DbError acc)

gaplessScan begin mapop (L.Fold foldop acc done) = do
  db' <- db
  ro' <- ro
  schema' <- schema
  let mapop' = mapop schema'
  records <- withIterator db' ro' $ \iter -> do
               if (isJust begin)
                 then iterSeek iter (fromJust begin)
                 else iterFirst iter
               -- Funnily enough, all the folloing is the equivalent of same thing, but all three yield
               -- different performance. For now, I'm sticking with leftEither
               -- let step !acc !x = mapop' x >>= \ !i -> strictFmap (\ !acc' -> foldop acc' i) acc
               -- let step !acc !x = liftA2 foldop acc (mapop' x)
               let step !acc !x = liftEither foldop acc (mapop' x)
               scanIntern iter step (Right acc)
  return $! fmap done records -- (records >>= (\x -> return $ done x))

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
            else return acc

-- TODO: add batch put operstiaon
-- TODO: add delete operation
