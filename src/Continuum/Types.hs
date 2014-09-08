{-# LANGUAGE DeriveGeneric #-}

module Continuum.Types where

import           Control.Monad.State.Strict
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

type AggregationFn acc = (acc -> (ByteString, ByteString) -> (Either DbError acc))

data DbType = DbtInt | DbtString
                       deriving(Show)

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
                           , schemaMappings :: Map.Map ByteString DbType
                           , schemaTypes    :: [DbType]
                           }

makeSchema :: [(ByteString, DbType)] -> DbSchema
makeSchema stringTypeList = DbSchema { fieldMappings = fMappings
                                     , fields = fields'
                                     , schemaMappings = Map.fromList stringTypeList
                                     , indexMappings = iMappings
                                     , schemaTypes = schemaTypes'}
  where fields' = fmap fst stringTypeList
        schemaTypes' = fmap snd stringTypeList
        fMappings = Map.fromList $ zip fields' iterateFrom0
        iMappings = Map.fromList $ zip iterateFrom0 fields'
        iterateFrom0 = (iterate (1+) 0)



data DbValue = DbInt Integer
             | DbFloat Float
             | DbDouble Double
             | DbString ByteString
             | DbTimestamp Integer
             | DbSequenceId Integer
             -- | DbList [DbValue]
             -- | DbMap [(DbValue, DbValue)]
             deriving (Show, Eq, Ord, Generic)

data DbRecord = DbRecord Integer (Map.Map ByteString DbValue) |
                DbPlaceholder Integer
                deriving(Show, Eq)
