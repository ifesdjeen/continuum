{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}

module Continuum.Types
       (module Continuum.Types
       , module Continuum.Common.Types
       )
       where

import           Continuum.Common.Types
import           Control.Monad.State.Strict
import           Control.Monad.Trans.Resource
import           Data.ByteString                ( ByteString )
-- import           Database.LevelDB.MonadResource ( DB, WriteOptions, ReadOptions )
import           Database.LevelDB.Base          ( DB, WriteOptions, ReadOptions )
import           GHC.Generics                   ( Generic )

import qualified Data.Map                       as Map
import qualified Data.Serialize                 as S
import qualified Data.Time.Clock.POSIX          as Clock

type SchemaMap     = Map.Map ByteString (DbSchema, DB)

type AppState a    = StateT DBContext IO (DbErrorMonad a)

type RWOptions     = (ReadOptions, WriteOptions)

-- |
-- | DB CONTEXT
-- |

data DBContext = DBContext
    { ctxSystemDb       :: DB
    , ctxDbs            :: Map.Map ByteString (DbSchema, DB)
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
-- | Cluster Related Data Types
-- |

data NodeStatus = NodeStatus {lastHeartbeat :: Clock.POSIXTime}
                deriving(Generic, Eq, Ord, Show)

type ClusterNodes = Map.Map Node NodeStatus
