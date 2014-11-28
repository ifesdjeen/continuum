{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}

module Continuum.Types
       (module Continuum.Types
       , module Continuum.Common.Types
       )
       where

import           Continuum.Common.Types
import           Control.Monad.State.Strict     ( StateT(..), MonadState(..), gets, modify, liftM )
import           Data.ByteString                ( ByteString )
import           Database.LevelDB.Base          ( DB, WriteOptions, ReadOptions )
import           GHC.Generics                   ( Generic )

import qualified Data.Map                       as Map
import qualified Data.Time.Clock.POSIX          as Clock

type AppState a    = StateT DbContext IO (DbErrorMonad a)

type RWOptions     = (ReadOptions, WriteOptions)

type ContextDbsMap = Map.Map ByteString (DbSchema, DB)

-- |
-- | DB CONTEXT
-- |

data DbContext = DbContext
    { ctxSystemDb    :: DB
    , ctxDbs         :: ContextDbsMap
    , ctxChunksDb    :: DB
    , ctxPath        :: String
    , sequenceNumber :: Integer
    , lastSnapshot   :: Integer
    , ctxRwOptions   :: RWOptions
    }

-- |Creates Getter, Mapper and Modifier accessors for the record
-- fields.
--
#define ACCESSORS(GETTER, MAPPER, MODIFIER, FIELD, FTYPE)         \
GETTER :: MonadState DbContext m => m FTYPE                     ; \
GETTER = gets FIELD                                             ; \
                                                                ; \
MAPPER :: (FTYPE -> FTYPE) -> DbContext  -> DbContext           ; \
MAPPER f a = a {FIELD = f (FIELD a) }                           ; \
                                                                ; \
MODIFIER :: MonadState DbContext m => (FTYPE -> FTYPE) ->  m () ; \
MODIFIER f = do                                                 ; \
  modify (MAPPER f)                                             ; \
  return ()

ACCESSORS(getCtxDbs,         fmapCtxDbs,         modifyCtxDbs,         ctxDbs,            ContextDbsMap)
ACCESSORS(getCtxChunksDb,    fmapCtxChunksDb,    modifyCtxChunksDb,    ctxChunksDb,       DB)
ACCESSORS(getCtxSystemDb,    fmapCtxSystemDb,    modifyCtxSystemDb,    ctxSystemDb,       DB)
ACCESSORS(getLastSnapshot,   fmapLastSnapshot,   modifyLastSnapshot,   lastSnapshot,      Integer)
ACCESSORS(getCtxPath,        fmapCtxPath,        modifyCtxPath,        ctxPath,           String)
ACCESSORS(getSequenceNumber, fmapSequenceNumber, modifySequenceNumber, sequenceNumber,    Integer)
#undef ACCESSORS

-- |
-- | Helper functions, additional accessors
-- |

-- |Check whether Database with the given name exist or no
--
dbExists :: MonadState DbContext m =>
            DbName
            -> m Bool
dbExists k = do
  db <- getDb k
  return $ case db of
    (Just _) ->  True
    (Nothing) -> False

-- |Retrieve database with the given name from Context.
--
getDb :: MonadState DbContext m =>
         DbName
         -> m (Maybe (DbSchema, DB))
getDb k = do
  dbs <- gets ctxDbs
  return $ Map.lookup k dbs

-- |Returns a database-unique monotonically incrementing sequence number.
getAndincrementSequence :: MonadState DbContext m
                           => m Integer
getAndincrementSequence = do
  old <- get
  _   <- modifySequenceNumber (+ 1)
  return $ sequenceNumber old

rwOptions :: MonadState DbContext m
             => m RWOptions
rwOptions = gets ctxRwOptions

getReadOptions :: MonadState DbContext m
                  => m ReadOptions
getReadOptions = liftM fst rwOptions

getWriteOptions :: MonadState DbContext m
                   => m WriteOptions
getWriteOptions = liftM snd rwOptions

-- |
-- | Cluster Related Data Types
-- |

data NodeStatus = NodeStatus {lastHeartbeat :: Clock.POSIXTime}
                deriving(Generic, Eq, Ord, Show)

type ClusterNodes = Map.Map Node NodeStatus
