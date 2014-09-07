{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord, findByTimestamp, findRange, gaplessScan,
        alwaysTrue)
       where

--  import Debug.Trace
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
import           Control.Monad.State
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
               scanIntern iter (\acc x -> (mapop schema' x) >>= (\y -> return $ foldop acc y)) acc
  return $ (records >>= (\x -> return $ done x))

scanIntern :: (MonadResource m) =>
               Iterator
               -> (acc -> (ByteString, ByteString) -> (Either DbError acc))
               -> acc
               -> m (Either DbError acc)

scanIntern iter op acc = do
          next <- iterEntry iter

          if isJust next
             then case op acc (fromJust next) of
                (Right res) -> do
                  iterNext iter
                  scanIntern iter op res
                val@(Left _) -> return val
             else return $ Right acc
{-# INLINE scanIntern #-}

-- TODO: add batch put operstiaon
-- TODO: add delete operation
