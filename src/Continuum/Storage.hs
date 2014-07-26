{-# LANGUAGE FlexibleContexts #-}

module Continuum.Storage where

import           Control.Applicative ((<$>))
import           Control.Monad.Trans.Resource
import qualified Database.LevelDB.Base  as Base
import           Database.LevelDB.Base (Options, DB, WriteOptions, ReadOptions)

import           Data.ByteString              (ByteString)


import Control.Monad.Reader


type RWOptions = (ReadOptions, WriteOptions)

data DBContext = DBContext { ctxDb :: DB
                             , ctxKeyspace :: ByteString
                             , ctxRwOptions :: RWOptions
                           }

-- withDatabase :: MonadResource m => FilePath -> Options -> m DB
-- withDatabase path opts = snd <$> open path opts

-- open :: MonadResource m => FilePath -> Options -> m (ReleaseKey, DB)
-- open path opts = allocate (Base.open path opts) Base.close

db :: MonadReader DBContext m => m DB
db = asks ctxDb

keyspace :: MonadReader DBContext m => m ByteString
keyspace = asks ctxKeyspace

rwOptions :: MonadReader DBContext m => m RWOptions
rwOptions = asks ctxRwOptions

ro :: MonadReader DBContext m => m ReadOptions
ro = liftM fst rwOptions

wo :: MonadReader DBContext m => m WriteOptions
wo = liftM snd rwOptions

put :: MonadReader DBContext IO => ByteString -> ByteString -> IO ()
put k v = do
  db <- db
  wo <- wo
  Base.put db wo  k v

get :: MonadReader DBContext IO => ByteString -> IO (Maybe ByteString)
get k = do
  db <- db
  ro <- ro
  Base.get db ro k
