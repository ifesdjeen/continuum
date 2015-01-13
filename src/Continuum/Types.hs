{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}

module Continuum.Types where

import           Control.Applicative            ( (<$>) )
import           Data.ByteString                ( ByteString )
import           GHC.Generics                   ( Generic )
import           Data.Maybe                     ( fromMaybe )
import           Data.ByteString.Char8          ( unpack )

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

data DbType =
  DbtLong
  | DbtInt
  | DbtByte
  | DbtShort
  | DbtFloat
  | DbtDouble
  | DbtString
  deriving(Show, Generic, Eq, Ord)

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
  | NoStepToResultConvertor
  | OtherError
  | NumericOperationError
  | NotEnoughInput Int Int
  deriving (Show, Eq, Ord, Generic)

instance S.Serialize DbError

type DbErrorMonad  = Either DbError


-- |
-- | DB VALUE
-- |

data DbValue =
  EmptyValue
  | DbString                ByteString
  | DbLong                  Integer
  | DbInt                   Integer
  | DbShort                 Integer
  | DbByte                  Integer
  | DbFloat                 Float
  | DbDouble                Double
  | DbList                  [DbValue]
  -- DbList [DbValue]
  -- DbMap [Map.Map DbValue DbValue]
  deriving (Eq, Ord, Generic)

withNumbers :: (Fractional a) =>
               [DbValue] ->
               ([a] -> a) ->
               DbErrorMonad a

withNumbers values op = op <$> mapM toNumber values

toNumber :: (Fractional a) => DbValue -> DbErrorMonad a
toNumber (DbString _) = Left NumericOperationError
toNumber (DbLong a)   = Right (fromInteger a)
toNumber (DbInt a)    = Right (fromIntegral a)
toNumber (DbShort a)  = Right (fromIntegral a)
toNumber (DbByte a)   = Right (fromIntegral a)
toNumber (DbFloat a)  = Right (realToFrac a)
toNumber (DbDouble a) = Right (realToFrac a)

instance Show DbValue where
  show EmptyValue   = ""
  show (DbInt v)    = show v
  show (DbLong v)   = show v
  show (DbShort v)  = show v
  show (DbString v) = unpack v
  show (DbFloat v)  = show v
  show (DbDouble v) = show v
  show (DbList v) =   unwords $ map show v

instance S.Serialize DbValue

data DbRecord =
  DbRecord Integer (Map.Map ByteString DbValue)
  deriving(Generic, Show, Eq)

instance S.Serialize DbRecord

getValue :: FieldName -> DbRecord -> DbValue
getValue fieldName (DbRecord _ fields) =
  fromMaybe EmptyValue (Map.lookup fieldName fields)

--
-- Stubs
--
instance Show (DbRecord -> DbValue) where
  show = undefined

instance S.Serialize (DbRecord -> DbValue) where
  put = undefined
  get = undefined

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
  | KeyRes                 Integer
  | ValueRes               DbValue
  | RecordRes              DbRecord
  | ListResult             [DbRecord]
  | MultiResult            (Map.Map ByteString DbResult)
  | MapResult              (Map.Map DbValue DbResult)
  -- TODO: RAW RESULT

  -- It looks like in the end, we can only get an empty result, error result,
  -- "raw" result (that covers things like key res and all other special cases),
  -- and record result (which overs both single and multi-field scenarios).

  -- TODO: Split Step and Res ??
  | DbSchemaResult         (DbName, DbSchema)
  deriving(Generic, Show, Eq)

-- It (only) seems to me that this split makes sense. For example, when
-- we have things like Limit or Skip steps that should keep some internal
-- state and then discard it after the step is finished. I'm not entirely
-- sure if we can avoid using the intermediate structure... Maybe we can tho.
data StepResult =
  EmptyStepRes
  | ErrorStepRes           DbError
  | CountStep              Integer
  | MinStep                DbValue
  | AvgStep                [DbValue]
  | ListStep               [DbRecord]
  | MultiStep              (Map.Map FieldName StepResult)
  | GroupStep              (Map.Map DbValue StepResult)
  deriving(Generic, Show, Eq)

instance S.Serialize DbResult

-- |
-- | RANGE
-- |

-- Maybe someday we'll need a ByteBuffer scan ranges. For now all keys are always
-- integers. Maybe iterators for something like indexes should be done somehow
-- differently not to make that stuff even more complex.
data ScanRange =
  OpenEnd                  Integer
  | KeyRange               Integer Integer
  | OpenEndButFirst        Integer
  | ButFirst               Integer Integer
  | ButLast                Integer Integer
  | EntireKeyspace
  deriving(Show, Generic)

instance S.Serialize ScanRange

-- |
-- | AGGREGATES
-- |

data Decoding =
  Field                    ByteString
  | Fields                 [ByteString]
  | Key
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
    } deriving (Generic, Show, Eq)

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

data SelectQuery =
  Count
  | Min                    FieldName
    -- | Max                    FieldName
    -- | Sum
  | Avg                    FieldName
  -- | Distinct
  | Multi                  [(FieldName, SelectQuery)]
  | Group                  (DbRecord -> DbValue)  SelectQuery
  | TimeFieldGroup         FieldName TimePeriod   SelectQuery
  | FieldGroup             FieldName              SelectQuery
  | TimeGroup                        TimePeriod   SelectQuery
  | FetchAll
  | Skip                   Integer
  | Limit                  Integer
  deriving (Generic, Show)

instance S.Serialize SelectQuery

-- |
-- | External Protocol Specification
-- |

-- TODO: Split client and server requests

data Node = Node String String
          deriving(Generic, Show, Eq, Ord)

instance S.Serialize Node

data Request =
  Shutdown
  | Insert                DbName DbRecord
  | CreateDb              DbName DbSchema
  -- TODO: Add ByteString here, never encode it inside of SelectQuery itself
  | Select                DbName SelectQuery
  deriving(Generic, Show)

instance S.Serialize Request

data TimePeriod =
  Milliseconds Integer |
  Seconds      Integer |
  Minutes      Integer |
  Hours        Integer |
  Days         Integer
  deriving(Generic, Show)

instance S.Serialize TimePeriod

roundTime :: TimePeriod -> DbRecord ->  DbValue
roundTime (Milliseconds slice) (DbRecord time _) = DbInt $ slice * (time `quot` slice)
roundTime (Seconds      slice) record            = roundTime (Milliseconds $ slice * 1000) record
roundTime (Minutes      slice) record            = roundTime (Seconds $ slice * 60) record
roundTime (Hours        slice) record            = roundTime (Minutes $ slice * 60) record
roundTime (Days         slice) record            = roundTime (Hours $ slice * 24) record
