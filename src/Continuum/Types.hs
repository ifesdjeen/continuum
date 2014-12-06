{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}

module Continuum.Types
       (module Continuum.Types
       , module Continuum.Common.Types
       )
       where

import           Control.Applicative    ( (<$>) )
import           Control.Concurrent.STM

import           Continuum.Common.Types
import           Data.ByteString                ( ByteString )
import           Database.LevelDB.Base          ( DB, WriteOptions, ReadOptions )
import           GHC.Generics                   ( Generic )
import           Data.Default.Class             ( Default(..) )

import qualified Continuum.Options              as Opts
import qualified Data.Map                       as Map
import qualified Data.Time.Clock.POSIX          as Clock

-- type AppState a    = StateT DbContext IO (DbErrorMonad a)

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
    , snapshotAfter  :: Integer
    }

defaultDbContext :: DbContext
defaultDbContext = DbContext
    {sequenceNumber = 1
    , lastSnapshot  = 1
    , ctxRwOptions  = (Opts.readOpts,
                       Opts.writeOpts)
    , snapshotAfter = 100000
    }

instance Default DbContext where
  def = defaultDbContext

-- |
-- | Cluster Related Data Types
-- |

data NodeStatus = NodeStatus {lastHeartbeat :: Clock.POSIXTime}
                deriving(Generic, Eq, Ord, Show)

type ClusterNodes = Map.Map Node NodeStatus


type ContextState = TVar DbContext

-- |Creates Getter, Mapper and Modifier accessors for the record
-- fields.
--
#define ACCESSORS(GETTER, MAPPER, MODIFIER, FIELD, FTYPE)         \
GETTER :: ContextState -> IO FTYPE                     ; \
GETTER s = FIELD <$> atomRead s                                              ; \
                                                                ; \
MAPPER :: (FTYPE -> FTYPE) -> DbContext  -> DbContext           ; \
MAPPER f a = a {FIELD = f (FIELD a) }                           ; \
                                                                ; \
MODIFIER :: (FTYPE -> FTYPE) -> ContextState ->  IO () ; \
MODIFIER f s = atomSwap (MAPPER f) s

ACCESSORS(getCtxDbs,         fmapCtxDbs,         modifyCtxDbs,         ctxDbs,            ContextDbsMap)
ACCESSORS(getCtxChunksDb,    fmapCtxChunksDb,    modifyCtxChunksDb,    ctxChunksDb,       DB)
ACCESSORS(getCtxSystemDb,    fmapCtxSystemDb,    modifyCtxSystemDb,    ctxSystemDb,       DB)
ACCESSORS(getLastSnapshot,   fmapLastSnapshot,   modifyLastSnapshot,   lastSnapshot,      Integer)
ACCESSORS(getCtxPath,        fmapCtxPath,        modifyCtxPath,        ctxPath,           String)
ACCESSORS(getSequenceNumber, fmapSequenceNumber, modifySequenceNumber, sequenceNumber,    Integer)
ACCESSORS(getCtxRwOptions,   fmapCtxRwOptions,   modifyCtxRwOptions,   ctxRwOptions,      RWOptions)
#undef ACCESSORS

-- |Check whether Database with the given name exist or no
--
dbExists :: ContextState
            -> DbName
            -> IO Bool
dbExists ctx k = do
  db <- getDb ctx k
  return $ case db of
    (Just _) ->  True
    (Nothing) -> False

-- |Retrieve database with the given name from Context.
--
getDb :: ContextState
         -> DbName
         -> IO (Maybe (DbSchema, DB))
getDb ctx k = do
  dbs <- getCtxDbs ctx
  return $ Map.lookup k dbs

-- |Returns a database-unique monotonically incrementing sequence number.
getAndincrementSequence :: ContextState -> IO Integer
getAndincrementSequence s = sequenceNumber <$> (atomReadSwap increment s)
  where increment old = old {sequenceNumber = ((sequenceNumber old) + 1) }

getReadOptions :: ContextState -> IO ReadOptions
getReadOptions st = fst <$> getCtxRwOptions st

getWriteOptions :: ContextState -> IO WriteOptions
getWriteOptions st = snd <$> getCtxRwOptions st


atomRead :: TVar a -> IO a
atomRead = atomically . readTVar

atomSwap :: (b -> b) -> TVar b -> IO ()
atomSwap f x = atomically $ modifyTVar x f

atomReadSwap :: (b -> b) -> TVar b -> IO b
atomReadSwap f x = atomically $ do
  v <- readTVar x
  _ <- modifyTVar x f
  return v

atomReset :: b -> TVar b -> IO ()
atomReset newv x = atomically $ writeTVar x newv
