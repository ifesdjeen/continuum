{-# LANGUAGE DeriveGeneric #-}

module Continuum.Common.Types where

import           Data.ByteString                ( ByteString )
import           GHC.Generics                   ( Generic )
import qualified Data.Serialize                 as S
import qualified Data.Map                       as Map

data DbType =
  DbtInt
  | DbtString
  deriving(Show, Generic)

instance S.Serialize DbType

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
-- |
-- |

data DbSchema = DbSchema
    { fieldMappings  :: Map.Map ByteString Int
    , fields         :: [ByteString]
    , indexMappings  :: Map.Map Int ByteString
    , schemaMappings :: Map.Map ByteString DbType
    , schemaTypes    :: [DbType]
    } deriving (Generic, Show)

instance S.Serialize DbSchema

-- |
-- | QUERIES
-- |

data Query =
  Count
  | Distinct
  | Min
  | Max
  | Group                 Query
  | Insert                ByteString ByteString
  | CreateDb              ByteString DbSchema
  | RunQuery              ByteString Decoding Query
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
  ImUp                    Node
  | Introduction          Node
  | NodeList              [Node]
  | Heartbeat             Node
  | Query                 Query
  deriving(Generic, Show)

instance S.Serialize Request
