{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}

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
import qualified Data.Time.Clock.POSIX as Clock

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

instance S.Serialize DbError

data DbType = DbtInt | DbtString
                       deriving(Show, Generic)

instance S.Serialize DbType

type RWOptions = (ReadOptions, WriteOptions)

-- |
-- | DB CONTEXT
-- |

data DBContext = DBContext { ctxSystemDb       :: DB
                           , ctxDbs            :: Map ByteString (DbSchema, DB)
                           , ctxNodes          :: ClusterNodes
                           , ctxSelfNode       :: Node
                           , ctxChunksDb       :: DB
                           , ctxPath           :: String
                           , sequenceNumber    :: Integer
                           , lastSnapshot      :: Integer
                             -- , ctxKeyspace  :: ByteString
                           , ctxRwOptions      :: RWOptions
                           }

#define ACCESSORS(GETTER, MAPPER, MODIFIER, FIELD, FTYPE)         \
GETTER :: MonadState DBContext m => m FTYPE                     ; \
GETTER = gets FIELD                                             ; \
                                                                ; \
MAPPER :: (FTYPE -> FTYPE) -> DBContext  -> DBContext           ; \
MAPPER f a = a {FIELD = f (FIELD a) }                           ; \
                                                                ; \
MODIFIER :: MonadState DBContext m => (FTYPE -> FTYPE) ->  m () ; \
MODIFIER f = do                                                 ; \
  modify (MAPPER f)                                             ; \
  return ()

ACCESSORS(getNodes, fmapNodes, modifyNodes, ctxNodes, ClusterNodes)
#undef ACCESSORS
-- ACCESSORS(getChunks, fmapChunks, modifyChunks, ctxChunks, DB)

getDb :: MonadState DBContext m => ByteString -> m (Maybe (DbSchema, DB))
getDb k = do
  dbs <- gets ctxDbs
  return $ Map.lookup k dbs

getChunks :: MonadState DBContext m => m DB
getChunks = gets ctxChunksDb

getSystemDb :: MonadState DBContext m => m DB
getSystemDb = gets ctxSystemDb

getPath :: MonadState DBContext m => m String
getPath = gets ctxPath

getAndincrementSequence :: MonadState DBContext m => m Integer
getAndincrementSequence = do
  old <- get
  modify (\a -> a {sequenceNumber = (sequenceNumber a) + 1})
  return $ sequenceNumber old

rwOptions :: MonadState DBContext m => m RWOptions
rwOptions = gets ctxRwOptions

getReadOptions :: MonadState DBContext m => m ReadOptions
getReadOptions = liftM fst rwOptions

getWriteOptions :: MonadState DBContext m => m WriteOptions
getWriteOptions = liftM snd rwOptions

-- |
-- | DB SCHEMA
-- |

data DbSchema = DbSchema { fieldMappings    :: Map ByteString Int
                           , fields         :: [ByteString]
                           , indexMappings  :: Map Int ByteString
                           , schemaMappings :: Map ByteString DbType
                           , schemaTypes    :: [DbType]
                           }
              deriving (Generic, Show)
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
              deriving(Generic, Show, Eq)

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

              deriving(Generic, Show, Eq)

instance S.Serialize DbResult
instance S.Serialize DbRecord
instance S.Serialize DbValue

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
              deriving(Generic, Show)

instance S.Serialize Decoding

-- |
-- | QUERIES
-- |

data Query = Count
           | Distinct
           | Min
           | Max
           | Group Query
           | Insert       ByteString ByteString
           | CreateDb     ByteString DbSchema
           | RunQuery     ByteString Decoding Query
           deriving (Generic, Show)


instance S.Serialize Query

-- |
-- | External Protocol Specification
-- |

data Request = ImUp Node
             | Introduction Node
             | NodeList     [Node]
             | Heartbeat    Node
             | Query        Query
             deriving(Generic, Show)

instance S.Serialize Request

-- |
-- | Cluster Related Data Types
-- |

data Node = Node String String
          deriving(Generic, Show, Eq, Ord)

data NodeStatus = NodeStatus {lastHeartbeat :: Clock.POSIXTime}
                deriving(Generic, Eq, Ord)

type ClusterNodes = Map.Map Node NodeStatus

instance S.Serialize Node
