{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord, findByTimestamp, findRange, scanAll)
       where
import           Continuum.Options
import           Continuum.Serialization
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
-- import qualified Data.ByteString        as BS
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

  let (k,v) = encodeRecord schema' record sid
  -- storagePut (encode (timestamp, sid)) value
  storagePut k v

  -- add default `empty` values (for Maybe)
  -- where value = encode $ fmap snd (Map.assocs record)

findByTimestamp :: Integer -> AppState [DbRecord]
findByTimestamp timestamp = scan (Just begin) id checker append []
                            where begin = encodeBeginTimestamp timestamp
                                  checker = compareTimestamps (==) timestamp

findRange :: Integer -> Integer -> AppState [DbRecord]
findRange beginTs end = scan (Just begin) id checker append []
                      where begin = encodeBeginTimestamp beginTs
                            checker = compareTimestamps (<=) end

scan :: Maybe ByteString
        -> (DbRecord -> i)
        -> (i -> acc -> Bool)
        -> (i -> acc -> acc)
        -> acc
        -> AppState acc

scan begin mapFn checker reduceFn accInit = do
  schema' <- schema
  scanRaw begin (mapFn . (decodeRecord schema')) checker reduceFn accInit

scanRaw :: Maybe ByteString
           -> ((ByteString, ByteString) -> i)
           -> (i -> acc -> Bool)
           -> (i -> acc -> acc)
           -> acc
           -> AppState acc
scanRaw begin mapFn checker reduceFn accInit = do
  db' <- db
  ro' <- ro
  records <- withIterator db' ro' $ \iter -> do
               if (isJust begin)
                 then iterSeek iter (fromJust begin)
                 else iterFirst iter
               scanIntern iter mapFn checker reduceFn accInit
  return $ records

scanIntern :: (MonadResource m)
        => Iterator
        -> ((ByteString, ByteString) -> i)
        -> (i -> acc -> Bool)
        -> (i -> acc -> acc)
        -> acc
        -> m acc

scanIntern iter mapFn checker reduceFn accInit = scanIntern accInit
  where scanIntern acc = do
          valid <- iterValid iter
          next <- iterEntry iter

          case (valid, next) of
            (True, Just (k, v)) -> do
              let mapped = mapFn (k, v)

              -- liftIO $ putStrLn (show v)

              if checker mapped acc
              then do
                () <- iterNext iter
                scanIntern $ reduceFn mapped acc
              else return acc
            _ -> return acc

scanAll :: (DbRecord -> i) -> (i -> acc -> acc) -> acc -> AppState acc
scanAll mapFn reduceFn acc = scan Nothing mapFn alwaysTrue reduceFn acc
                             where alwaysTrue = \_ _ -> True

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
