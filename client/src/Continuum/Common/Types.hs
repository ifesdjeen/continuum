{-# LANGUAGE DeriveGeneric #-}

module Continuum.Common.Types where

import           Data.ByteString                ( ByteString )
import           GHC.Generics                   ( Generic )
import qualified Data.Serialize                 as S
import qualified Data.Map                       as Map

-- |
-- | INTERNAL DB TYPES
-- |

data DbType =
  DbtInt
  | DbtDouble
  | DbtString
  deriving(Show, Generic)

instance S.Serialize DbType

-- |
-- | DB Error
-- |

data DbError =
  IndexesDecodeError        String
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

instance S.Serialize DbError

type DbErrorMonad  = Either  DbError


-- |
-- | DB VALUE
-- |

data DbValue =
  EmptyValue
  | DbInt                   Integer
  | DbFloat                 Float
  | DbDouble                Double
  | DbString                ByteString
  | DbTimestamp             Integer
  | DbSequenceId            Integer
  -- DbList [DbValue]
  -- DbMap [(DbValue, DbValue)]
  deriving (Show, Eq, Ord, Generic)

instance S.Serialize DbValue

data DbRecord =
  DbRecord Integer (Map.Map ByteString DbValue)
  deriving(Generic, Show, Eq)

instance S.Serialize DbRecord

-- | Creates a DbRecord from Timestamp and Key/Value pairs
--
makeRecord :: Integer -> [(ByteString, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

-- |
-- | DB RESULT
-- |

data DbResult =
  EmptyRes
  | ErrorRes               DbError
  | RecordRes              DbRecord
  | FieldRes               (Integer, DbValue)
  | FieldsRes              (Integer, [DbValue])
  | CountStep              Integer
  | CountRes               Integer
  | GroupRes               (Map.Map DbValue DbResult)
  | DbResults              [DbResult]
  deriving(Generic, Show, Eq)

instance S.Serialize DbResult

-- |
-- | RANGE
-- |

data KeyRange =
  OpenEnd                  ByteString
  | TsOpenEnd              Integer
  | SingleKey              ByteString
  | TsSingleKey            Integer
  | KeyRange               ByteString ByteString
  | TsKeyRange             Integer Integer
  | EntireKeyspace
  deriving(Show)

-- |
-- | AGGREGATES
-- |

data Decoding =
  Field                    ByteString
  | Fields                 [ByteString]
  | Record
  deriving(Generic, Show)

instance S.Serialize Decoding

-- |
-- | DB SCHEMA
-- |

data DbSchema = DbSchema
    { fieldMappings  :: Map.Map ByteString Int
    , fields         :: [ByteString]
    , indexMappings  :: Map.Map Int ByteString
    , schemaMappings :: Map.Map ByteString DbType
    , schemaTypes    :: [DbType]
    } deriving (Generic, Show)

instance S.Serialize DbSchema

-- | Creates a DbSchema out of Schema Definition (name/type pairs)
--
makeSchema :: [(ByteString, DbType)] -> DbSchema
makeSchema stringTypeList =
  DbSchema { fieldMappings  = fMappings
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
-- | QUERIES
-- |

data Query =
  Count
  | Distinct
  | Min
  | Max
  | FetchAll              ByteString
  | Group                 Query
  | Insert                ByteString DbRecord
  | CreateDb              ByteString DbSchema
  deriving (Generic, Show)

instance S.Serialize Query

-- |
-- | External Protocol Specification
-- |

-- TODO: Split client and server requests

data Node = Node String String
          deriving(Generic, Show, Eq, Ord)

instance S.Serialize Node

data Request =
  Shutdown
  | ImUp                  Node
  | Introduction          Node
  | NodeList              [Node]
  | Heartbeat             Node
  | RunQuery              Query
  deriving(Generic, Show)

instance S.Serialize Request
