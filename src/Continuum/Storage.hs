{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp,
        putRecord,
        alwaysTrue,
        scan,
        createDatabase,
        runAppState,
        initializeDbs
        )
       where

-- import           Debug.Trace

import           Continuum.Options
import           Continuum.Serialization
import           Continuum.Types

import           Control.Monad.Except
import           Control.Monad.State.Strict
import           Control.Monad.Trans.Resource

import qualified Database.LevelDB.MonadResource as LDB
import qualified Data.ByteString.Char8          as C8
import qualified Data.ByteString                as BS
import qualified Data.Map.Strict                as Map
import qualified Control.Foldl                  as L

import           Data.Traversable               ( traverse )
import           Data.Maybe                     ( isJust, fromJust )
import           Control.Applicative            ( Applicative(..) , (<$>), (<*>) )
import           Database.LevelDB.MonadResource ( DB, Iterator )
import           Data.ByteString                ( ByteString )

putRecord :: ByteString -> DbRecord -> AppState DbResult
putRecord dbName record = do
  sid          <- getAndincrementSequence
  _            <- maybeWriteChunk sid record
  maybeDbDescr <- getDb dbName
  case maybeDbDescr of
    (Just (schema, db)) -> do
      wo <- getWriteOptions
      -- TODO: read up about bracket
      let (key, value) = encodeRecord schema record sid
      LDB.put db wo key value
      return $ return $ EmptyRes
    Nothing             -> return $ Left NoSuchDatabaseError

putSchema :: ByteString -> DbSchema -> AppState DbResult
putSchema dbName sch = do
  sysDb   <- getSystemDb
  wo      <- getWriteOptions
  LDB.put (sysDb) wo dbName (encodeSchema sch)
  return $ Right EmptyRes

alwaysTrue :: a -> b -> Bool
alwaysTrue = \_ _ -> True

-- prepareContext path dbSchema =

runApp :: String
          -> AppState a
          -> IO (Either DbError a)
runApp path actions = do
  runResourceT $ do
    -- TODO: add indexes
    systemDb  <- LDB.open (path ++ "/system") opts
    chunksDb  <- LDB.open (path ++ "/chunksDb") opts
    eitherDbs <- initializeDbs path systemDb
    let host    = "127.0.0.1"
        port    = "4444"
        context dbs =   DBContext {ctxPath           = path,
                               ctxSystemDb       = systemDb,
                               ctxNodes          = Map.empty,
                               ctxSelfNode       = Node host port,
                               ctxDbs            = dbs,
                               ctxChunksDb       = chunksDb,
                               sequenceNumber    = 1,
                               lastSnapshot      = 1,
                               ctxRwOptions      = (readOpts, writeOpts)}
    let run dbs = evalStateT actions (context dbs)
    join <$> traverse run eitherDbs

runAppState :: DBContext -> AppState a -> IO (DbErrorMonad a,
                                              DBContext)
runAppState  st op = runResourceT . runStateT op $ st

liftDbError :: (a -> b -> c)
              -> DbErrorMonad a
              -> DbErrorMonad b
              -> DbErrorMonad c
liftDbError f (Right a) (Right b) = return $! f a b
liftDbError _ (Left a)  _         = Left a
liftDbError _ _         (Left b)  = Left b

-- TODO: add batch put operstiaon
-- TODO: add delete operation

scan :: ByteString
        -> KeyRange
        -> Decoding
        -> L.Fold DbResult acc
        -> AppState acc

scan dbName keyRange decoding (L.Fold foldop acc done) = do
  maybeDb <- getDb dbName
  -- TODO: generalize
  if (isJust maybeDb)
    then do
    ro <- getReadOptions
    let (schema, db) = fromJust maybeDb
    records <- LDB.withIterator db ro $ \iter -> do
      let mapop        = decodeRecord decoding schema
          getNext      = advanceIterator iter keyRange
          step !a !x   = liftDbError foldop a (mapop x)
      setStartPosition iter keyRange
      scanStep getNext step (Right acc)
    return $! fmap done records
    else do
    return $ Left NoSuchDatabaseError

scanStep :: (MonadResource m) =>
            m (Maybe (ByteString, ByteString))
            -> (acc -> (ByteString, ByteString) -> acc)
            -> acc
            -> m acc

scanStep getNext op orig = s orig
  where s !acc = do
          next <- getNext
          if isJust next
            then s (op acc (fromJust next))
            else return acc

-- | Advances iterator to a single entry, exits and returns nothing in case there's either nothing
-- more to read or we've reached the end of Key Range
advanceIterator :: MonadResource m =>
                   Iterator
                   -> KeyRange
                   -> m (Maybe (ByteString, ByteString))
advanceIterator iter (KeyRange _ rangeEnd) = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  LDB.iterNext iter
  return $ (,) <$> (maybeInterrupt mkey) <*> mval
  where maybeInterrupt k = k >>= interruptCondition
        interruptCondition resKey = if resKey <= rangeEnd
                                    then Just resKey
                                    else Nothing

advanceIterator iter (TsKeyRange _ rangeEnd) =
  advanceIterator iter (KeyRange BS.empty (encodeEndTimestamp rangeEnd))

advanceIterator iter (SingleKey singleKey) = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  LDB.iterNext iter
  return $ (,) <$> (maybeInterrupt mkey) <*> mval
  where maybeInterrupt k = k >>= interruptCondition
        interruptCondition resKey = if resKey == singleKey
                                    then Just resKey
                                    else Nothing

advanceIterator iter (TsSingleKey intKey) = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  LDB.iterNext iter
  return $ (,) <$> (maybeInterrupt mkey) <*> mval
  where maybeInterrupt k = k >>= interruptCondition
        encodedKey = packWord64 intKey
        interruptCondition resKey = if (BS.take 8 resKey) == encodedKey
                                    then Just resKey
                                    else Nothing


advanceIterator iter _ = do
  mkey <- LDB.iterKey iter
  mval <- LDB.iterValue iter
  LDB.iterNext iter
  return $ (,) <$> mkey <*> mval

setStartPosition :: MonadResource m =>
                    Iterator
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

maybeWriteChunk :: Integer -> DbRecord -> AppState DbResult
maybeWriteChunk sid (DbRecord time _) = do
  st <- get
  when (sid >= (lastSnapshot st) + snapshotAfter || sid == 1) $ do
    modify (\a -> a {lastSnapshot = sid})
    LDB.put (ctxChunksDb st) (snd (ctxRwOptions st)) (packWord64 time) BS.empty
  return $ return $ EmptyRes
maybeWriteChunk _ _ = return $ throwError OtherError

initializeDbs :: MonadResource m =>
                 String
                 -> DB
                 -> m (DbErrorMonad SchemaMap)
initializeDbs path systemDb = do
  schemas <- LDB.withIterator systemDb readOpts $ readSchemas
  traverse (addDbInit path) (sequence schemas)
  where
    readSchemas i  = map decodeSchema <$> (LDB.iterFirst i >> LDB.iterItems i)

addDbInit :: MonadResource m =>
             String
             -> [(ByteString, DbSchema)]
             -> m SchemaMap
addDbInit path coll =
  Map.fromList <$> mapM initDb coll

  where initDb (dbName, sch) = do
          -- TODO: abstract path finding
          ldb <- LDB.open (path ++ "/" ++ (C8.unpack dbName)) opts
          return (dbName, (sch, ldb))

createDatabase :: ByteString
                  -> DbSchema
                  -> AppState DbResult
createDatabase dbName sch = do
  path <- getPath
  -- Add uniqueness check
  ldb  <- LDB.open (path ++ "/" ++ (C8.unpack dbName)) opts
  _    <- putSchema dbName sch
  modify (\a -> a {ctxDbs = Map.insert dbName (sch, ldb) (ctxDbs a)})
  return $ Right $ EmptyRes
