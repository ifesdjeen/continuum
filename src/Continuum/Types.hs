{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE FlexibleInstances         #-}
module Continuum.Types ( module Continuum.Types
                       , MapResult(..)
                       , Stream(..)
                       , Step(..)

                       , KeyRange(..)
                       , Direction(..)
                       , Value
                       , Entry

                       , BatchOp(..)
                       , WriteBatch
                       , DB
                       , Iterator) where

import qualified Data.Serialize                 as S
import qualified Data.Map                       as Map

import GHC.Exception              ( Exception )
import Data.ByteString            ( ByteString )
import GHC.Generics               ( Generic )
import Data.Stream.Monadic        ( Step(..), Stream(..) )
import Database.LevelDB.Base      ( WriteBatch, BatchOp(..), DB )
import Database.LevelDB.Streaming ( KeyRange(..), Direction(..), Value, Entry)
import Database.LevelDB.Iterator  ( Iterator )

-- |
-- | ALIASES
-- |

type ChunkKey      = ByteString
type DbKey         = ByteString
type DbName        = ByteString
type FieldName     = ByteString
type EncodedValue  = ByteString

-- |
-- | INTERNAL DB TYPES
-- |

-- | DBType represent Schema types
data DbType =
  DbtLong
  | DbtInt
  | DbtByte
  | DbtShort
  | DbtFloat
  | DbtDouble
  | DbtString
  deriving(Show, Generic, Eq, Ord)

-- | DBValue represen Schema instances of Schema types
data DbValue =
  DbString                  ByteString
  | DbLong                  Integer
  | DbInt                   Integer
  | DbShort                 Integer
  | DbByte                  Integer
  | DbFloat                 Float
  | DbDouble                Double
  deriving (Eq, Show, Ord, Generic)

newtype MapResult a b = MapResult (Map.Map a b)
                      deriving (Show, Ord, Eq)

instance (Ord k, Monoid v) => Monoid (MapResult k v) where
  mempty  = MapResult $ Map.empty
  mappend (MapResult a) (MapResult b) = MapResult $ Map.unionWith mappend a b

-- |
-- | DB SCHEMA
-- |

data DbRecord =
  DbRecord Integer (Map.Map ByteString DbValue)
  deriving(Generic, Show, Eq)

-- |
-- | DB SCHEMA
-- |

data DbSchema = DbSchema
    { fieldMappings  :: Map.Map ByteString Int
    , fields         :: [ByteString]
    , indexMappings  :: Map.Map Int ByteString
    , schemaMappings :: Map.Map ByteString DbType
    , schemaTypes    :: [DbType]
    } deriving (Generic, Show, Eq)

-- |
-- | Chunks
-- |


-- |
-- | DB Error
-- |

data Decoding =
  Field                    ByteString
  | Fields                 [ByteString]
  | Key
  | Record
  deriving(Generic, Show)


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
  | NoStepToResultConvertor
  | OtherError
  | NumericOperationError
  | NotEnoughInput Int Int
  deriving (Show, Eq, Ord, Generic)

instance Exception DbError

-- |
-- | Base Storage
-- |

data TimeRange =
  TimeBetween Integer Integer
  | AllTime
  deriving(Show, Generic)

-- |
-- | Serialize Instances
-- |

instance S.Serialize DbType
instance S.Serialize DbSchema
