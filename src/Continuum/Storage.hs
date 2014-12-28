{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE RecordWildCards   #-}

module Continuum.Storage
       where

-- import           Debug.Trace                    ( trace )

import           Continuum.Context
import           Continuum.Common.Types
import           Continuum.Common.Primitive
import           Continuum.Options
import           Continuum.Common.Serialization

import           Control.Monad.Except
import qualified Continuum.NewStorage           as NewStorage
import qualified Database.LevelDB.Base          as LDB
import qualified Data.ByteString.Char8          as C8
import qualified Data.ByteString                as BS
import qualified Data.Map.Strict                as Map
import qualified Continuum.Options              as Opts
import qualified Control.Foldl                  as L

import           Data.Traversable               ( traverse )
import           System.Process                 ( system )
import           Control.Applicative            ( (<$>) )

-- |
-- | OPERATIONS
-- |

-- |Insert Record into the Database by given name
--
putRecord :: DbName
             -> DbRecord
             -> DbState DbResult
putRecord dbName record = do
  sid          <- getAndincrementSequence
  _            <- maybeWriteChunk sid record
  maybeDbDescr <- getDb dbName

  case maybeDbDescr of
    (Just (schema, db)) -> do
      wo <- getWriteOptions
      let (key, value) = encodeRecord schema record sid
      _  <- LDB.put db wo key value

      return $ return $ EmptyRes
    Nothing             -> return $ Left NoSuchDatabaseError

-- |Insert Schema Record of an existing database by
-- given name. Databases are re-initialized on startup
-- from System DB. Every registered database should have
-- a corresponding record in System DB
--
putSchema :: DbName
             -> DbSchema
             -> DbState DbResult
putSchema dbName sch = do
  sysDb   <- getCtxSystemDb
  wo      <- getWriteOptions
  _       <- LDB.put sysDb wo dbName (encodeSchema sch)
  return $ Right EmptyRes

-- TODO: add batch put operstiaon
-- TODO: add delete operation

-- |
-- | Chunking / Query Parallelisation
-- |

maybeWriteChunk :: Integer
                   -> DbRecord
                   -> DbState DbResult
maybeWriteChunk sid (DbRecord time _) = do
  DbContext{..} <- readT
  when (sid >= lastSnapshot + snapshotAfter || sid == 1) $ do
    modifyLastSnapshot $ const sid
    LDB.put ctxChunksDb (snd ctxRwOptions) (packWord64 time) BS.empty
  return $ return $ EmptyRes

-- |
-- | DATABASE INITIALIZATION
-- |

-- |Initialize all the existing databases, registered in _systemdb_
--
initializeDbs :: String
                 -> LDB.DB
                 -> IO (DbErrorMonad ContextDbsMap)
initializeDbs path systemDb = do
  dbs <- NewStorage.scan systemDb readOpts EntireKeyspace decodeSchema
  res <- traverse (mapM (initializeDb path)) dbs
  return $ Map.fromList <$> res

-- |Initialize (open) an instance of an _existing_ database.
--
initializeDb :: String
                -> (DbName, DbSchema)
                -> IO (DbName, (DbSchema, LDB.DB))
initializeDb path (dbName, sch) = do
  -- TODO: abstract path finding
  ldb <- LDB.open (path ++ "/" ++ (C8.unpack dbName)) opts
  return (dbName, (sch, ldb))

-- |Create a database if it does not yet exist.
-- Database is returned in an open state, ready for writes.
--
createDatabase :: DbName
                  -> DbSchema
                  -> DbState DbResult
createDatabase dbName sch = do
  exists <- dbExists dbName
  if (not exists)
    then createDb
    else return $ Right EmptyRes

  where createDb = do
          path <- getCtxPath
          ldb  <- LDB.open (path ++ "/" ++ (C8.unpack dbName)) opts
          _    <- putSchema dbName sch
          _    <- modifyCtxDbs $ Map.insert dbName (sch, ldb)
          return $ Right EmptyRes


-- |
-- | AUXILITARY FUNCTIONS
-- |

liftDbError :: (a -> b -> c)
              -> DbErrorMonad a
              -> DbErrorMonad b
              -> DbErrorMonad c
liftDbError f (Right a) (Right b) = return $! f a b
liftDbError _ (Left a)  _         = Left a
liftDbError _ _         (Left b)  = Left b


-- |
-- | Startup
-- |


startStorage :: String -> DbContext -> IO DbContext
startStorage path ctx = do
  _           <- system ("mkdir " ++ path)
  systemDb    <- LDB.open (path ++ "/system") Opts.opts
  chunksDb    <- LDB.open (path ++ "/chunksDb") Opts.opts

  -- TODO: add Error Handling!
  (Right dbs) <- initializeDbs path systemDb

  return $ ctx  {ctxPath           = path,
                 ctxSystemDb       = systemDb,
                 ctxDbs            = dbs,
                 ctxChunksDb       = chunksDb }

stopStorage :: DbContext -> IO ()
stopStorage DbContext{..} = do
  _             <- LDB.close ctxSystemDb
  _             <- LDB.close ctxChunksDb
  _             <- mapM (\(_, (_, db)) -> LDB.close db) (Map.toList ctxDbs)
  return ()

readChunks :: ScanRange -> DbState [Integer]
readChunks scanRange = do
  db     <- getCtxChunksDb
  ro     <- getReadOptions
  chunks <- lift $ NewStorage.scan db ro scanRange decodeChunkKey
  return chunks

scan :: DbContext
        -> DbName
        -> ScanRange
        -> Decoding
        -> L.Fold DbRecord acc
        -> IO (DbErrorMonad acc)

scan context dbName scanRange decoding foldOp = do
  let maybeDb = Map.lookup dbName (ctxDbs context)
      ro      = fst $ ctxRwOptions context

  case maybeDb of
    (Just (schema, db)) -> do
      scanRes <- NewStorage.scan db ro scanRange (decodeRecord decoding schema)
      return $ L.fold foldOp <$> scanRes
    Nothing             -> return $ Left NoSuchDatabaseError
