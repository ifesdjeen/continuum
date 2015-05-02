{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}

module Continuum.Types where

import           Data.ByteString                   ( ByteString )
import           GHC.Generics                      ( Generic )

import qualified Data.Serialize                 as S
import qualified Data.Map                       as Map

-- |
-- | ALIASES
-- |

type DbName        = ByteString
type FieldName     = ByteString

type Decoder a     = (ByteString, ByteString) -> DbErrorMonad a

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

type DbErrorMonad  = Either DbError

-- |
-- | Serialize Instances
-- |

instance S.Serialize DbType
instance S.Serialize DbSchema
