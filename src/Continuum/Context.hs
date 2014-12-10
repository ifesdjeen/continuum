{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}

module Continuum.Context where

import           Continuum.Common.Types

import qualified Continuum.Options              as Opts
import qualified Data.Map                       as Map

import           Control.Monad.State.Strict     ( StateT(..), get )
import           Control.Monad.State.Class      ( MonadState )
import           Control.Monad.IO.Class         ( MonadIO(..), liftIO )
import           Control.Concurrent.STM         ( TVar, atomically, readTVar, writeTVar, newTVar, modifyTVar)
import           Control.Applicative            ( (<$>) )
import           Data.ByteString                ( ByteString )
import           Database.LevelDB.Base          ( DB, WriteOptions, ReadOptions )
import           Data.Default.Class             ( Default(..) )

-- |
-- | Aliases
-- |

type DbDryRun  = StateT (TVar DbContext) IO ()

type DbState a = StateT (TVar DbContext) IO (DbErrorMonad a)

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
    , sequenceNumber :: Integer -- TODO: Make it per-database ...
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

type ContextState = TVar DbContext

-- |
-- | Getters / Mappers / Modifiers
-- |

#define ACCESSORS(GETTER, MAPPER, MODIFIER, FIELD, FTYPE)         \
GETTER :: (Functor m, MonadIO m, (MonadState (TVar DbContext) m)) => m FTYPE                    ; \
GETTER = FIELD <$> readT                                                             ; \
                                                                                     ; \
MAPPER :: (FTYPE -> FTYPE) -> DbContext  -> DbContext                                ; \
MAPPER f a = a {FIELD = f (FIELD a) }                                                ; \
                                                                                     ; \
MODIFIER :: (MonadIO m, (MonadState (TVar DbContext) m)) => (FTYPE -> FTYPE) -> m () ; \
MODIFIER f = modifyT (MAPPER f)

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
dbExists :: (Functor m, MonadIO m, MonadState (TVar DbContext) m) =>
            DbName
            -> m Bool
dbExists ctx = do
  db <- getDb ctx
  return $ case db of
    (Just _) ->  True
    (Nothing) -> False

-- |Retrieve database with the given name from Context.
--
getDb :: (Functor m, MonadIO m, MonadState (TVar DbContext) m) =>
         DbName
         -> m (Maybe (DbSchema, DB))
getDb k = do
  dbs <- getCtxDbs
  return $ Map.lookup k dbs

-- |Returns a database-unique monotonically incrementing sequence number.
getAndincrementSequence :: (Functor m, MonadIO m, (MonadState (TVar DbContext) m)) =>
                           m Integer
getAndincrementSequence = sequenceNumber <$> (readAndModifyT increment)
  where increment old = old {sequenceNumber = ((sequenceNumber old) + 1) }

getReadOptions :: (Functor m, MonadIO m, (MonadState (TVar DbContext) m)) =>
                  m ReadOptions
getReadOptions = fst <$> getCtxRwOptions

getWriteOptions :: (Functor m, MonadIO m, (MonadState (TVar DbContext) m)) =>
                   m WriteOptions
getWriteOptions = snd <$> getCtxRwOptions

-- |
-- | Atom-related
-- |

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

atomInit :: b -> IO (TVar b)
atomInit v = atomically $ newTVar v

-- |
-- | StateT + Atom
-- |

readAndModifyT :: (MonadIO m, (MonadState (TVar DbContext) m)) =>
                  (DbContext -> DbContext)
                  -> m DbContext
readAndModifyT f = do
  tvar <- get
  res    <- liftIO $ atomReadSwap f tvar
  return $ res

modifyT :: (MonadIO m, (MonadState (TVar DbContext) m)) =>
           (DbContext -> DbContext)
           -> m ()
modifyT f = do
  tvar <- get
  _    <- liftIO $ atomSwap f tvar
  return $ ()

readT :: (MonadIO m, (MonadState (TVar DbContext) m)) => m DbContext
readT = do
  tvar <- get
  res  <- liftIO $ atomRead tvar
  return res
