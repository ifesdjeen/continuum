{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}

module Continuum.Storage where

import           Continuum.Serialization
-- import           Control.Applicative ((<$>))
-- import           Control.Monad.Trans.Resource

import           Data.Serialize (Serialize, encode, decode)
import qualified Data.Map as Map

import qualified Database.LevelDB.Base  as Base
import           Database.LevelDB.Base (DB, WriteOptions, ReadOptions,
                                        iterSeek, iterFirst, iterItems, withIter)
import           Data.Default
import           Data.ByteString              (ByteString)
import           Control.Monad.Reader

type RWOptions = (ReadOptions, WriteOptions)

data DBContext = DBContext { ctxDb          :: DB
                             , ctxSchema    :: DbSchema
                             -- , ctxKeyspace  :: ByteString
                             , ctxRwOptions :: RWOptions
                           }

makeContext :: DB -> DbSchema -> RWOptions -> DBContext
makeContext db schema rwOptions = DBContext {ctxDb = db,
                                             ctxSchema = schema,
                                             ctxRwOptions = rwOptions}

-- withDatabase :: MonadResource m => FilePath -> Options -> m DB
-- withDatabase path opts = snd <$> open path opts

-- open :: MonadResource m => FilePath -> Options -> m (ReleaseKey, DB)
-- open path opts = allocate (Base.open path opts) Base.close

db :: MonadReader DBContext m => m DB
db = asks ctxDb

-- keyspace :: MonadReader DBContext m => m ByteString
-- keyspace = asks ctxKeyspace

schema :: MonadReader DBContext m => m DbSchema
schema = asks ctxSchema

rwOptions :: MonadReader DBContext m => m RWOptions
rwOptions = asks ctxRwOptions

ro :: MonadReader DBContext m => m ReadOptions
ro = liftM fst rwOptions

wo :: MonadReader DBContext m => m WriteOptions
wo = liftM snd rwOptions

putDbValue :: MonadReader DBContext IO => DbValue -> DbValue -> IO ()
putDbValue k v = do
  db' <- db
  wo' <- wo
  Base.put db' wo' (encode k) (encode v)

putDbValue' :: DbValue -> DbValue -> ReaderT DBContext IO ()
putDbValue' k v = do
  db' <- db
  wo' <- wo
  Base.put db' wo' (encode k) (encode v)



getDbValue :: MonadReader DBContext IO => DbValue -> IO (Maybe ByteString)
getDbValue k = do
  db' <- db
  ro' <- ro
  Base.get db' ro' (encode k)

--- WTF is difference between that and the other one???
getDbValue' :: DbValue -> ReaderT DBContext IO (Maybe ByteString)
getDbValue' k = do
  db' <- db
  ro' <- ro
  Base.get db' ro' (encode k)


putRecord :: MonadReader DBContext IO => DbRecord -> IO ()
putRecord (DbRecord timestamp sequenceId record) = do
  db' <- db
  wo' <- wo
  Base.put db' wo' key value
  where key = encode (timestamp, sequenceId)
        value = encode $ fmap snd (Map.assocs record)

putRecord' :: DbRecord -> ReaderT DBContext IO ()
putRecord' (DbRecord timestamp sequenceId record) = do
  db' <- db
  wo' <- wo
  Base.put db' wo' key value
  where key = encode (timestamp, sequenceId)
        value = encode $ fmap snd (Map.assocs record)


findTs' :: DB -> ReadOptions -> Integer -> IO [(ByteString, ByteString)]
findTs' db' ro' timestamp = do
  withIter db' ro' $ \iter -> do
    iterSeek iter (encode timestamp)
    iterItems iter

findTs :: MonadReader DBContext IO => Integer -> IO [DbRecord]
findTs timestamp = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- findTs' db' ro' timestamp
  return $ makeDbRecords schema' records
