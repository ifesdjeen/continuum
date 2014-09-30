{-# LANGUAGE DeriveGeneric #-}

module Continuum.Types where

import qualified Data.Map as Map

import           Control.Monad.State.Strict
import           Data.ByteString        (ByteString)
import           Control.Monad.Trans.Resource
import           Database.LevelDB.MonadResource (DB,
                                                 WriteOptions,
                                                 ReadOptions)
import           Data.Map (Map)
import qualified Data.Serialize as S
import           GHC.Generics           (Generic)

-- type DbErrorMonadT = ExceptT DbError IO
type DbErrorMonad  = Either  DbError

type SchemaMap     = Map ByteString (DbSchema, DB)

type AppState a = StateT DBContext (ResourceT IO) (DbErrorMonad a)

data DbError = IndexesDecodeError      String
             | FieldDecodeError        String ByteString
             | ValuesDecodeError       String
             | ValueDecodeError        String
             | KeyDecodeError          String
             | DecodeFieldByIndexError String [Int]
             | FieldNotFoundError
             | NoSuchDatabaseError
             | NoAggregatorAvailable
             | SchemaDecodingError     String
             | OtherError
             deriving (Show, Eq, Ord, Generic)

data DbType = DbtInt | DbtString
                       deriving(Show, Generic)

instance S.Serialize DbType

type RWOptions = (ReadOptions, WriteOptions)

-- |
-- | DB CONTEXT
-- |

data DBContext = DBContext { ctxSystemDb       :: DB
                           , ctxDbs            :: Map ByteString (DbSchema, DB)
                           , ctxChunksDb       :: DB
                           , ctxPath           :: String
                           , sequenceNumber    :: Integer
                           , lastSnapshot      :: Integer
                             -- , ctxKeyspace  :: ByteString
                           , ctxRwOptions      :: RWOptions
                           }

-- |
-- | DB SCHEMA
-- |

data DbSchema = DbSchema { fieldMappings    :: Map ByteString Int
                           , fields         :: [ByteString]
                           , indexMappings  :: Map Int ByteString
                           , schemaMappings :: Map ByteString DbType
                           , schemaTypes    :: [DbType]
                           }
              deriving (Generic)
instance S.Serialize DbSchema


makeSchema :: [(ByteString, DbType)] -> DbSchema
makeSchema stringTypeList = DbSchema { fieldMappings  = fMappings
                                     , fields         = fields'
                                     , schemaMappings = Map.fromList stringTypeList
                                     , indexMappings  = iMappings
                                     , schemaTypes    = schemaTypes'}
  where fields'      = fmap fst stringTypeList
        schemaTypes' = fmap snd stringTypeList
        fMappings    = Map.fromList $ zip fields' iterateFrom0
        iMappings    = Map.fromList $ zip iterateFrom0 fields'
        iterateFrom0 = (iterate (1+) 0)

-- |
-- | DB VALUE
-- |

data DbValue = EmptyValue
             | DbInt Integer
             | DbFloat Float
             | DbDouble Double
             | DbString ByteString
             | DbTimestamp Integer
             | DbSequenceId Integer
             -- | DbList [DbValue]
             -- | DbMap [(DbValue, DbValue)]
             deriving (Show, Eq, Ord, Generic)

data DbRecord = DbRecord Integer (Map ByteString DbValue)
                deriving(Show, Eq)

makeRecord :: Integer -> [(ByteString, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

-- |
-- | DB RESULT
-- |

data DbResult = EmptyRes
              | ErrorRes     DbError
              | RecordRes    DbRecord
              | FieldRes     (Integer, DbValue)
              | FieldsRes    (Integer, [DbValue])

              | CountStep    Integer
              | CountRes     Integer
              | GroupRes     (Map DbValue DbResult)

              deriving(Show, Eq)

-- |
-- | RANGE
-- |

data KeyRange = OpenEnd          ByteString
                | TsOpenEnd      Integer
                | SingleKey      ByteString
                | TsSingleKey    Integer
                | KeyRange       ByteString ByteString
                | TsKeyRange     Integer Integer
                | EntireKeyspace
                deriving(Show)
-- |
-- | AGGREGATES
-- |

data Decoding = Field  ByteString
              | Fields [ByteString]
              | Record

-- |
-- | QUERIES
-- |

data Query = Count
           | Distinct
           | Min
           | Max
           | Group Query
           deriving (Show)
