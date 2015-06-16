{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}

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

import qualified Data.Serialize  as S
import qualified Data.Map        as Map
import qualified Data.Aeson      as Json
import qualified Data.Vector     as V

import GHC.Exception              ( Exception )
import Data.ByteString            ( ByteString )
import GHC.Generics               ( Generic )
import Data.Stream.Monadic        ( Step(..), Stream(..) )
import Control.Applicative        ( (<*>) )
import Data.Aeson                 ( ToJSON, FromJSON, toJSON, (.=), (.:) )
import Data.Aeson.Types           ( Parser, parseMaybe )
import Data.Text.Encoding         ( decodeUtf8, encodeUtf8 )

import Database.LevelDB.Base      ( WriteBatch, BatchOp(..), DB, Options(..), defaultOptions )
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

-- | Creates a DbRecord from Timestamp and Key/Value pairs
--
makeRecord :: Integer -> [(FieldName, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

-- |
-- | DB SCHEMA
-- |

data DbSchema = DbSchema
    { fieldMappings  :: Map.Map FieldName Int
    , fields         :: [FieldName]
    , indexMappings  :: Map.Map Int FieldName
    , schemaMappings :: Map.Map FieldName DbType
    , nameTypeList   :: [(FieldName, DbType)]
    , schemaTypes    :: [DbType]
    } deriving (Generic, Show, Eq)

-- | Creates a DbSchema out of Schema Definition (name/type pairs)
--
makeSchema :: [(ByteString, DbType)] -> DbSchema
makeSchema stringTypeList =
  DbSchema { fieldMappings  = fMappings
           , fields         = fields'
           , schemaMappings = Map.fromList stringTypeList
           , nameTypeList   = stringTypeList
           , indexMappings  = iMappings
           , schemaTypes    = schemaTypes'}
  where fields'      = fmap fst stringTypeList
        schemaTypes' = fmap snd stringTypeList
        fMappings    = Map.fromList $ zip fields' iterateFrom0
        iMappings    = Map.fromList $ zip iterateFrom0 fields'
        iterateFrom0 = (iterate (1+) 0)

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

-- |
-- | DbResult
-- |

data DbResult =
  OK
  deriving (Show)


-- |
-- | HTTP Encoding
-- |

instance ToJSON DbType
instance FromJSON DbType

-- instance Json.ToJSON ByteString where
--   toJSON ()

instance Json.FromJSON ByteString where
  parseJSON (Json.String t) = pure . encodeUtf8 $ t
  parseJSON _               = mempty

instance ToJSON ByteString where
  toJSON = toJSON . decodeUtf8

instance Json.FromJSON DbSchema where
  parseJSON (Json.Array v) = makeSchema <$> (sequence (fmap Json.parseJSON (V.toList v)))
  parseJSON _ = mempty

-- instance Json.FromJSON DbRecord where
--   parseJSON (Json.Object v) = makeRecord <$>
--                               v .: "timestamp" <*>
--                               v .: "data"
--   parseJSON _ = mempty

decodeDbtFromJson :: DbType -> Json.Value -> Parser DbValue
decodeDbtFromJson DbtInt (Json.Number a) = return $ DbInt 1
decodeDbtFromJson DbtLong (Json.Number a) = return $ DbLong 1
decodeDbtFromJson _ _ = mempty

recordFromJson :: DbSchema -> Json.Object -> Parser DbRecord
recordFromJson schema value = do
  ts    <- value .: "timestsamp"
  d     <- value .: "data"
  -- types <- mapM (\(n, tp) -> ((,) tp) <$> d .: (decodeUtf8 n)) (nameTypeList schema)
  types <- mapM (\(n, tp) -> ((,) tp) <$> d .: (decodeUtf8 n)) (nameTypeList schema)
  r     <- mapM (uncurry decodeDbtFromJson) types
  return $ makeRecord ts (zip (fields schema) r)

instance ToJSON DbValue where
  toJSON (DbInt v)    = toJSON v
  toJSON (DbLong v)   = toJSON v
  toJSON (DbShort v)  = toJSON v
  toJSON (DbString v) = toJSON $ decodeUtf8 v
  toJSON (DbFloat v)  = toJSON v
  toJSON (DbDouble v) = toJSON v

instance Json.ToJSON DbRecord where
  toJSON (DbRecord i m) = Json.object ["timestamp" .= i,
                                       "data"      .= Map.toList m]

instance Json.ToJSON DbSchema where
  toJSON schema = toJSON (nameTypeList schema)

-- |
-- | Default Options
-- |

defaultOpts :: Options
defaultOpts = defaultOptions{ createIfMissing = True
                            , cacheSize= 512 * 1048576
                                         -- , blockSize= 2048
                                         -- , compression = NoCompression
                                         -- , compression = NoCompressionNoCompression
                                         -- , comparator = Just customComparator
                        }

sch = makeSchema [("a", DbtLong) ]
rec = makeRecord 1 [("a", DbLong 1)]

a = (\i -> parseMaybe (recordFromJson sch) i) <$> (Json.decode (Json.encode rec) :: Maybe Json.Object)
