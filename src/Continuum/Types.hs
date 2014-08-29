{-# LANGUAGE DeriveGeneric #-}

module Continuum.Types where

import           Control.Monad.State
import           Data.ByteString        (ByteString)
import           GHC.Generics           (Generic)
import           Control.Monad.Trans.Resource
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator)

import qualified Data.Map as Map

type AppState a = StateT DBContext (ResourceT IO) a

data DbError = IndexesDecodeError String
             | FieldDecodeError String ByteString
             | ValuesDecodeError String
             | ValueDecodeError String
             | KeyDecodeError String
             | FieldNotFoundError
             | DecodeFieldByIndexError String [Int]
             | OtherError
             deriving (Show, Eq, Ord, Generic)

type AggregationFn acc = ((ByteString, ByteString) -> acc -> (Either DbError acc))

data DbType = DbtInt | DbtString

-- type AggregationPipeline acc = ((ByteString, ByteString) -> acc -> (Either String acc))


type RWOptions = (ReadOptions, WriteOptions)

-- | DB CONTEXT

data DBContext = DBContext { ctxDb          :: DB
                             , ctxSchema    :: DbSchema
                             , sequenceNumber :: Integer
                             -- , ctxKeyspace  :: ByteString
                             , ctxRwOptions :: RWOptions
                           }

-- | DB SCHEMA

data DbSchema = DbSchema { fieldMappings    :: Map.Map ByteString Int
                           , fields         :: [ByteString]
                           , indexMappings  :: Map.Map Int ByteString
                           , schemaMappings :: Map.Map ByteString DbType }

makeSchema :: [(ByteString, DbType)] -> DbSchema
makeSchema stringTypeList = DbSchema { fieldMappings = fMappings
                                     , fields = fields'
                                     , schemaMappings = Map.fromList stringTypeList
                                     , indexMappings = iMappings }
  where fields' = fmap fst stringTypeList
        fMappings = Map.fromList $ zip fields' iterateFrom0
        iMappings = Map.fromList $ zip iterateFrom0 fields'
        iterateFrom0 = (iterate (1+) 0)
