{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage where

import           Continuum.Serialization
import           Control.Applicative ((<$>))
-- import           Control.Monad.Trans.Resource

import           Data.Serialize (Serialize, encode, decode)
import qualified Data.Map as Map

import qualified Database.LevelDB.Base  as Base
import           Database.LevelDB.Base (DB, WriteOptions, ReadOptions,
                                        iterSeek, iterFirst, iterItems, withIter,
                                        iterNext, mapIter, iterEntry)
import           Database.LevelDB.Iterator (Iterator)
import           Data.Default
import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS
-- import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Trans.Maybe
import           Data.Maybe


import           Database.LevelDB.C

type RWOptions = (ReadOptions, WriteOptions)

data DBContext = DBContext { ctxDb          :: DB
                             , ctxSchema    :: DbSchema
                             , sequenceNumber :: Integer
                             -- , ctxKeyspace  :: ByteString
                             , ctxRwOptions :: RWOptions
                           }

tsPlaceholder :: ByteString
tsPlaceholder = encode (DbString "____placeholder" :: DbValue)

makeContext :: DB -> DbSchema -> RWOptions -> DBContext
makeContext db schema rwOptions = DBContext {ctxDb = db,
                                             sequenceNumber = 1,
                                             ctxSchema = schema,
                                             ctxRwOptions = rwOptions}

-- withDatabase :: MonadResource m => FilePath -> Options -> m DB
-- withDatabase path opts = snd <$> open path opts

-- open :: MonadResource m => FilePath -> Options -> m (ReleaseKey, DB)
-- open path opts = allocate (Base.open path opts) Base.close

db :: MonadState DBContext m => m DB
db = gets ctxDb

getAndincrementSequence :: MonadState DBContext m => m Integer
getAndincrementSequence = do
  old <- get
  -- put old {sequenceNumber = (sequenceNumber old) + 1}
  modify (\a -> a {sequenceNumber = (sequenceNumber a) + 1})
  return $ sequenceNumber old

-- modify (\a -> a {sequenceNumber = (sequenceNumber a) + 1})

getSequence :: MonadState DBContext m => m Integer
getSequence = gets sequenceNumber


-- keyspace :: MonadState DBContext m => m ByteString
-- keyspace = asks ctxKeyspace

schema :: MonadState DBContext m => m DbSchema
schema = gets ctxSchema

rwOptions :: MonadState DBContext m => m RWOptions
rwOptions = gets ctxRwOptions

ro :: MonadState DBContext m => m ReadOptions
ro = liftM fst rwOptions

wo :: MonadState DBContext m => m WriteOptions
wo = liftM snd rwOptions

putDbValue :: (Integer, Integer) -> DbValue -> StateT DBContext IO ()
putDbValue k v = do
  db' <- db
  wo' <- wo
  Base.put db' wo' (encode k) (encode v)


getDbValue :: DbValue -> StateT DBContext IO (Maybe ByteString)
getDbValue k = do
  db' <- db
  ro' <- ro
  Base.get db' ro' (encode k)

getDbValue2 :: Integer -> StateT DBContext IO (Maybe ByteString)
getDbValue2 k = do
  db' <- db
  ro' <- ro
  Base.get db' ro' (encode (k, -1 :: Integer))

storagePut :: ByteString -> ByteString -> StateT DBContext IO ()
storagePut key value = do
  db' <- db
  wo' <- wo
  Base.put db' wo' key value

putRecord :: DbRecord -> StateT DBContext IO ()
putRecord (DbRecord timestamp record) = do
  sid <- getAndincrementSequence -- :: StateT DBContext IO (Maybe Integer)

  encoded <- return $ encode (timestamp, sid)

  -- Write empty timestamp to be used for iteration, overwrite if needed
  storagePut (encode (timestamp, 0 :: Integer)) tsPlaceholder
  -- Write an actual value to the database

  liftIO $ putStrLn ("=>> Written: " ++ (show (timestamp, sid)))

  storagePut (encode (timestamp, sid)) value

  where value = encode $ fmap snd (Map.assocs record)

-- findTs' :: DB -> ReadOptions -> Integer -> IO [(ByteString, ByteString)]
-- findTs' db' ro' timestamp = do
--   withIter db' ro' $ \iter -> do
--     iterSeek iter (encode timestamp)
--     iterItems iter

-- findTs :: Integer -> StateT DBContext IO [DbRecord]
-- findTs timestamp = do
--   db' <- db
--   ro' <- ro
--   schema' <- schema
--   records <- liftIO $ findTs' db' ro' timestamp
--   return $ makeDbRecords schema' records

findTs :: Integer -> StateT DBContext IO [DbRecord]
findTs timestamp = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIter db' ro' $ \iter -> do
               iterSeek iter (encode (timestamp, 0 :: Integer))
               -- iterNext iter
               iterWhile iter (isSame timestamp)

  return $ makeDbRecords schema' records


isSame :: Integer -> (ByteString, ByteString) -> Bool
isSame end (key, _) = k == end
                      where (k, _) = (decodeKey key)

reachedEnd :: Integer -> (ByteString, ByteString) -> Bool
reachedEnd end (key, _) = k < end
                          where (k, _) = (decodeKey key)
                                -- (encode (end, 0 :: Integer)) /= key

findRange :: Integer -> Integer -> StateT DBContext IO [DbRecord]
findRange begin end = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIter db' ro' $ \iter -> do
               iterSeek iter (encode (begin, 0 :: Integer))
               iterWhile iter (reachedEnd end)
               -- untilM (reachedEnd end) (\x -> iterNext iter)
               -- where reachedEnd (key _) = true
               -- iterItems iter

  -- liftIO $ putStrLn (show records)

  return $ makeDbRecords schema' records


iterWhile :: (Functor m, MonadIO m) => Iterator -> ((ByteString, ByteString) -> Bool)-> m [(ByteString, ByteString)]
iterWhile iter checker = (takeWhile checker) <$> (catMaybes <$> mapIter iterEntry iter)

iterUntil :: (Functor m, MonadIO m) => Iterator -> ((ByteString, ByteString) -> Bool)-> m [(ByteString, ByteString)]
iterUntil iter checker = (takeWhile (not . checker)) <$> (catMaybes <$> mapIter iterEntry iter)


compareKeys :: ByteString -> ByteString -> Ordering
compareKeys k1 k2 = compare k11 k21
                    where (k11, k12) = decodeKey k1
                          (k21, k22) = decodeKey k2


findAll :: StateT DBContext IO [DbRecord]
findAll = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIter db' ro' $ \iter -> do
               iterFirst iter
               iterItems iter

  -- liftIO $ putStrLn (show records)

  return $ makeDbRecords schema' records


a :: [Int] -> [Int]
a l = go [] l
      where
        go acc (s:xs) = if null xs
                           then (s:acc)
                           else go (s:acc) xs
