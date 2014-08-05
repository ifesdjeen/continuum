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
-- import           Control.Monad.Reader
import           Control.Monad.State

type RWOptions = (ReadOptions, WriteOptions)

data DBContext = DBContext { ctxDb          :: DB
                             , ctxSchema    :: DbSchema
                             , sequenceNumber :: Integer
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

db :: MonadState DBContext m => m DB
db = gets ctxDb

incrementSequence :: MonadState DBContext m => m ()
incrementSequence = modify (\a -> a {sequenceNumber = (sequenceNumber a) + 1})

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

putDbValue :: DbValue -> DbValue -> StateT DBContext IO ()
putDbValue k v = do
  db' <- db
  wo' <- wo
  Base.put db' wo' (encode k) (encode v)


--- WTF is difference between that and the other one???
getDbValue :: DbValue -> StateT DBContext IO (Maybe ByteString)
getDbValue k = do
  db' <- db
  ro' <- ro
  Base.get db' ro' (encode k)


putRecord :: DbRecord -> StateT DBContext IO ()
putRecord (DbRecord timestamp sequenceId record) = do
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

findTs :: Integer -> StateT DBContext IO [DbRecord]
findTs timestamp = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- liftIO $ findTs' db' ro' timestamp
  return $ makeDbRecords schema' records
