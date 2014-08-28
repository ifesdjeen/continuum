{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord, findByTimestamp, findRange, scanAll,
        alwaysTrue)
       where

import Debug.Trace
import Continuum.Types
import           Control.Monad.Trans.Maybe
import           Continuum.Options
import           Continuum.Serialization
import           Control.Monad.Error
-- import           Control.Applicative ((<$>))

-- import           Data.Serialize (Serialize, encode, decode)
import           Data.Serialize (encode)
-- import qualified Data.Map as Map

import qualified Database.LevelDB.MonadResource  as Base
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterSeek, iterFirst, -- iterItems,
                                                 withIterator, iterNext, iterEntry,
                                                 iterValid)
---import           Database.LevelDB.Iterator (Iterator)


import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS
import           Control.Monad.State
import           Data.Maybe (isJust, fromJust)


import           Control.Monad.Trans.Resource


type AppState a = StateT DBContext (ResourceT IO) a
-- type AppResource a = ResourceT AppState IO a

type RWOptions = (ReadOptions, WriteOptions)

data DBContext = DBContext { ctxDb          :: DB
                             , ctxSchema    :: DbSchema
                             , sequenceNumber :: Integer
                             -- , ctxKeyspace  :: ByteString
                             , ctxRwOptions :: RWOptions
                           }

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

getSequence :: MonadState DBContext m => m Integer
getSequence = gets sequenceNumber

schema :: MonadState DBContext m => m DbSchema
schema = gets ctxSchema

rwOptions :: MonadState DBContext m => m RWOptions
rwOptions = gets ctxRwOptions

ro :: MonadState DBContext m => m ReadOptions
ro = liftM fst rwOptions

wo :: MonadState DBContext m => m WriteOptions
wo = liftM snd rwOptions

tsPlaceholder :: ByteString
tsPlaceholder = encode (DbString "____placeholder" :: DbValue)

storagePut :: ByteString -> ByteString -> AppState ()
storagePut key value = do
  db' <- db
  wo' <- wo
  Base.put db' wo' key value

putRecord :: DbRecord -> AppState ()
putRecord record@(DbRecord timestamp _) = do
  sid <- getAndincrementSequence -- :: StateT DBContext IO (Maybe Integer)
  schema' <- schema

  -- Write empty timestamp to be used for iteration, overwrite if needed
  storagePut (encode (timestamp, 0 :: Integer)) tsPlaceholder
  -- Write an actual value to the database

  liftIO $ when (sid `mod` 1000 == 0) $ do putStrLn ("=>> Written: " ++ (show (timestamp, sid)))

  -- let (k,v) = encodeRecord schema' record sid
  let (k,v) = indexingEncodeRecord schema' record sid
  -- storagePut (encode (timestamp, sid)) value
  storagePut k v

  -- add default `empty` values (for Maybe)
  -- where value = encode $ fmap snd (Map.assocs record)

findByTimestamp :: Integer -> AppState (Either String [DbRecord])
findByTimestamp timestamp = scan (Just begin) (withFullRecord id checker append) []
                            where begin   = encodeBeginTimestamp timestamp
                                  checker = compareTimestamps (==) timestamp

findRange :: Integer -> Integer -> AppState (Either String [DbRecord])
findRange beginTs end = scan (Just begin) (withFullRecord id checker append) []
                      where begin = encodeBeginTimestamp beginTs
                            checker = compareTimestamps (<=) end

alwaysTrue :: a -> b -> Bool
alwaysTrue = \_ _ -> True

runApp :: String -> DbSchema -> AppState a -> IO (a)
runApp path schema' actions = do
  runResourceT $ do
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

-- Full record aggregation Pipeline,
-- Receives a decoded record, passes it through mapper, puts into
-- accumulator until `checker` returns true.
withFullRecord ::
          (DbRecord -> i)
       -> (i -> acc -> Bool)
       -> (i -> acc -> acc)
       -> DbSchema
       -> AggregationFn acc

withFullRecord mapFn checker reduceFn schema (k, v) acc = do
  a <- decodeRecord schema (k, v)
  let mapped = mapFn a
  if checker mapped acc
    then return $ reduceFn mapped acc
    else return $ acc


scanAll :: (Eq acc, Show acc) =>
              (DbRecord -> i)
           -> (i -> acc -> acc)
           -> acc
           -> AppState (Either String acc)
scanAll mapFn reduceFn = scan Nothing (withFullRecord mapFn alwaysTrue reduceFn)

getNextItem :: (MonadResource m) =>
               Iterator
            -> m (Maybe (ByteString, ByteString))

getNextItem iter = do
  next <- iterEntry iter

  case next of
    Just (k, v) | v == tsPlaceholder ->
      iterNext iter >> getNextItem iter

    val@(Just (k, v)) -> return $ val

    _ -> return $ Nothing



scan :: (Eq acc, Show acc) =>
         Maybe ByteString
         -> (DbSchema -> AggregationFn acc)
         -> acc
         -> AppState (Either String acc)
scan begin op acc = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIterator db' ro' $ \iter -> do
               if (isJust begin)
                 then iterSeek iter (fromJust begin)
                 else iterFirst iter
               scanIntern iter (op schema') acc
  return $ records



scanIntern :: (Eq acc, Show acc, MonadResource m) =>
               Iterator
            -> AggregationFn acc
            -> acc
            -> m (Either String acc)

scanIntern iter op acc = do
          next <- iterEntry iter

          case next of
            Just (k, v) | v == tsPlaceholder ->
              iterNext iter >> scanIntern iter op acc

            Just (k, v) ->
              case op (k, v) acc of
                (Right res) -> do
                               iterNext iter
                               if res == acc
                                 then return $ Right res
                                 else scanIntern iter op res
                val@(Left a) -> return val
            _ -> return $ Right acc
