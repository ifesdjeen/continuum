{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE RecordWildCards   #-}

module Continuum.Storage
       where

import           Continuum.Types
import           Continuum.Options
import           Continuum.Common.Serialization
import           Control.Monad.Except


import qualified Database.LevelDB.Base          as LDB
import qualified Data.ByteString.Char8          as C8
import qualified Data.ByteString                as BS
import qualified Data.Map.Strict                as Map
import qualified Continuum.Options              as Opts

import           Control.Concurrent.STM         ( TVar, newTVar, atomically, readTVar )
import           Control.Exception.Base         ( bracket )
import           Control.Foldl                  ( Fold(..) )
import           Control.Monad.State.Strict     ( get )
import           Data.Traversable               ( traverse )
import           System.Process                 ( system )
import           Data.Maybe                     ( isJust, fromJust )
import           Control.Applicative            ( Applicative(..) , (<$>), (<*>) )
import           Data.ByteString                ( ByteString )

-- |
-- | OPERATIONS
-- |

-- |Insert Record into the Database by given name
--
putRecord :: DbName
             -> DbRecord
             -> AppState DbResult
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
             -> AppState DbResult
putSchema dbName sch = do
  sysDb   <- getCtxSystemDb
  wo      <- getWriteOptions
  _       <- LDB.put sysDb wo dbName (encodeSchema sch)
  return $ Right EmptyRes

-- TODO: add batch put operstiaon
-- TODO: add delete operation

scan :: DbName
        -> KeyRange
        -> Decoding
        -> Fold DbResult acc
        -> AppState acc
scan dbName keyRange decoding foldOp = do
  maybeDb <- getDb dbName
  case maybeDb of
    (Just (schema, db)) -> scanDb db keyRange (decodeRecord decoding schema) foldOp
    Nothing             -> return $ Left NoSuchDatabaseError

-- |Perform a Scan operation.
--
--   * @KeyRange@ specifies where the given scan should start, and until when
--     it should be scanning.
--   * @Decoding@ specifies what / how to deserialize (single @Field@, many
--     @Fields@, or an entire @DbRecord@
--   * @Fold@     specifies which Fold to use (Group, Append and so on, for
--     complex querying)
--
scanDb :: LDB.DB
          -> KeyRange
          -> Decoder
          -> Fold DbResult acc
          -> AppState acc

scanDb db keyRange decoder (Fold foldop acc done) = do
  ro <- getReadOptions
  records <- LDB.withIter db ro $ \iter -> do
    let getNext      = advanceIterator iter keyRange
        step !a !x   = liftDbError foldop a (decoder x)
    setStartPosition iter keyRange
    scanStep getNext step (Right acc)
  return $! fmap done records

-- |Perform a single step of the Scan Operation. Combines
-- @decodeRecord@ and @advanceIterator@, prepared in @scan@,
-- recurses into itself until @advanceIterator@ returns @Just@
-- something.
--
scanStep :: (MonadIO m) =>
            m (Maybe (ByteString, ByteString))
            -> (acc -> (ByteString, ByteString) -> acc)
            -> acc
            -> m acc

scanStep getNext op orig = recur orig
  where recur !acc = do
          next <- getNext
          if isJust next
            then recur (op acc (fromJust next))
            else return acc

-- | Advances iterator for _just one_ step, exits and returns nothing in
-- case there's either nothing more to read or we've reached the end of
-- @KeyRange@.
--
-- Exit (interrupt) condition depends on the given @KeyRange@. For example,
-- @OpenEnd@ doesn't exit until all the entries are read from database.
--
advanceIterator :: MonadIO m =>
                   LDB.Iterator
                   -> KeyRange
                   -> m (Maybe (ByteString, ByteString))
advanceIterator iter (KeyRange _ rangeEnd) = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  _    <- LDB.iterNext iter

  return $ (,) <$> (maybeInterrupt mkey) <*> mval

  where maybeInterrupt k = k >>= condition
        condition resKey = if resKey <= rangeEnd
                           then Just resKey
                           else Nothing

advanceIterator iter (TsKeyRange _ rangeEnd) =
  advanceIterator iter (KeyRange BS.empty (encodeEndTimestamp rangeEnd))

advanceIterator iter (SingleKey singleKey) = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  _    <- LDB.iterNext iter

  return $ (,) <$> (maybeInterrupt mkey) <*> mval

  where maybeInterrupt k = k >>= condition
        condition resKey = if resKey == singleKey
                           then Just resKey
                           else Nothing

advanceIterator iter (TsSingleKey intKey) = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  _    <- LDB.iterNext iter

  return $ (,) <$> (maybeInterrupt mkey) <*> mval

  where maybeInterrupt k = k >>= condition
        encodedKey       = packWord64 intKey
        condition resKey = if (BS.take 8 resKey) == encodedKey
                           then Just resKey
                           else Nothing


advanceIterator iter _ = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  _    <- LDB.iterNext iter
  return $ (,) <$> mkey <*> mval

-- |Sets start position of @Iterator@ depending on @KeyRange@ type.
-- Every Range has some start position.
--
setStartPosition :: MonadIO m =>
                    LDB.Iterator
                    -> KeyRange
                    -> m ()

setStartPosition iter (OpenEnd startPosition) =
  LDB.iterSeek iter startPosition
setStartPosition iter (TsOpenEnd startPosition)  =
  setStartPosition iter (OpenEnd (encodeBeginTimestamp startPosition))

setStartPosition iter (SingleKey startPosition)  =
  LDB.iterSeek iter startPosition
setStartPosition iter (TsSingleKey startPosition)  =
  setStartPosition iter (SingleKey (encodeBeginTimestamp startPosition))

setStartPosition iter (KeyRange startPosition _) =
  LDB.iterSeek iter startPosition
setStartPosition iter (TsKeyRange startPosition _) =
  setStartPosition iter (KeyRange (encodeBeginTimestamp startPosition) BS.empty)

setStartPosition iter EntireKeyspace = LDB.iterFirst iter

-- |
-- | Chunking / Query Parallelisation
-- |

snapshotAfter :: Integer
snapshotAfter = 250000

maybeWriteChunk :: Integer
                   -> DbRecord
                   -> AppState DbResult
maybeWriteChunk sid (DbRecord time _) = do
  DBContext{..} <- get
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
  schemas <- LDB.withIter systemDb readOpts readSchemas
  traverse initAll (sequence schemas)
  where
    readSchemas i = map decodeSchema <$> (LDB.iterFirst i >> LDB.iterItems i)
    initAll coll  = Map.fromList <$> mapM (initializeDb path) coll

-- |Initialize (open) an instance of an _existing_ database.
--
initializeDb :: String
                -> DbResult
                -> IO (DbName, (DbSchema, LDB.DB))
initializeDb path (DbSchemaResult (dbName, sch)) = do
  -- TODO: abstract path finding
  ldb <- LDB.open (path ++ "/" ++ (C8.unpack dbName)) opts
  return (dbName, (sch, ldb))

initializeDb _ _ = error "can't initialize the database"

-- |Create a database if it does not yet exist.
-- Database is returned in an open state, ready for writes.
--
createDatabase :: DbName
                  -> DbSchema
                  -> AppState DbResult
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


startStorage :: String -> IO (TVar DBContext)
startStorage path = do
  _           <- system ("mkdir " ++ path)
  systemDb    <- LDB.open (path ++ "/system") Opts.opts
  chunksDb    <- LDB.open (path ++ "/chunksDb") Opts.opts

  (Right dbs) <- initializeDbs path systemDb

  let context = DBContext {ctxPath           = path,
                           ctxSystemDb       = systemDb,
                           ctxDbs            = dbs,
                           ctxChunksDb       = chunksDb,
                           sequenceNumber    = 1,
                           lastSnapshot      = 1,
                           ctxRwOptions      = (Opts.readOpts,
                                                Opts.writeOpts)}

  shared <- atomically $ newTVar context

  return shared

stopStorage :: TVar DBContext -> IO ()
stopStorage shared = do
  DBContext{..} <- atomRead shared
  _             <- LDB.close ctxSystemDb
  _             <- LDB.close ctxChunksDb
  _             <- mapM (\(_, (_, db)) -> LDB.close db) (Map.toList ctxDbs)
  return ()
  where atomRead = atomically . readTVar

withStorage :: String
               -> (TVar DBContext -> IO a)
               -> IO a
withStorage path subsystem = do
  bracket (startStorage path)
          stopStorage
          subsystem
