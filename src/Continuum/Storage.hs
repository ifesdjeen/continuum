{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord,
        alwaysTrue, scan, parallelScan)
       where

-- import           Debug.Trace

import           Continuum.Options
import           Continuum.Serialization
import           Continuum.Types
import           Control.Applicative ((<$>), (<*>))
import           Control.Concurrent.ParallelIO.Global
import           Control.Monad.Except
import qualified Data.Map.Strict as Map
import           Data.Monoid
import           Data.Traversable (traverse)
import qualified Database.LevelDB.MonadResource  as Base
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterItems,
                                                 iterKey, iterValue,
                                                 iterSeek, iterFirst, -- iterItems,
                                                 withIterator, iterNext, iterKeys)
import           Data.ByteString                (ByteString)
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString as BS
import           Control.Monad.State.Strict
import           Data.Maybe                     (isJust, fromJust)
import           Control.Monad.Trans.Resource
import qualified Control.Foldl as L

makeContext :: DB
            -> Map.Map ByteString (DbSchema, DB)
            -> DB
            -> DbSchema
            -> RWOptions
            -> DBContext
makeContext systemDb dbs chunksDb dbSchema rwOptions' =
  DBContext {ctxSystemDb       = systemDb,
             ctxDbs            = dbs,
             ctxChunksDb       = chunksDb,
             sequenceNumber    = 1,
             lastSnapshot      = 1,
             ctxSchema         = dbSchema,
             ctxRwOptions      = rwOptions'}


db :: MonadState DBContext m => ByteString -> m (Maybe (DbSchema, DB))
db k = do
  dbs <- gets ctxDbs
  return $ Map.lookup k dbs

chunks :: MonadState DBContext m => m DB
chunks = gets ctxChunksDb

getAndincrementSequence :: MonadState DBContext m => m Integer
getAndincrementSequence = do
  old <- get
  modify (\a -> a {sequenceNumber = (sequenceNumber a) + 1})
  return $ sequenceNumber old

schema :: MonadState DBContext m => m DbSchema
schema = gets ctxSchema

rwOptions :: MonadState DBContext m => m RWOptions
rwOptions = gets ctxRwOptions

ro :: MonadState DBContext m => m ReadOptions
ro = liftM fst rwOptions

wo :: MonadState DBContext m => m WriteOptions
wo = liftM snd rwOptions

storagePut :: ByteString -> (ByteString, ByteString) -> AppState DbResult
storagePut dbName (key, value) = do
  db' <- db dbName
  if (isJust db')
    then do
    wo' <- wo
    -- TODO: read up about bracket
    Base.put (snd $ fromJust db') wo' key value
    return $ return $ EmptyRes
    else
    return $ Left NoSuchDatabaseError

putRecord :: ByteString -> DbRecord -> AppState DbResult
putRecord dbName record = do
  sid     <- getAndincrementSequence
  _       <- maybeWriteChunk sid record
  schema' <- schema
  storagePut dbName (encodeRecord schema' record sid)

alwaysTrue :: a -> b -> Bool
alwaysTrue = \_ _ -> True

-- prepareContext path dbSchema =

runApp :: String -> DbSchema
          -> AppState a
          -> IO (Either DbError a)
runApp path dbSchema actions = do
  runResourceT $ do
    -- TODO: add indexes
    systemDb  <- Base.open (path ++ "/system") opts
    chunksDb  <- Base.open (path ++ "/chunksDb") opts
    eitherDbs <- initializeDbs path systemDb
    let run dbs = evalStateT actions (makeContext systemDb dbs chunksDb dbSchema (readOpts, writeOpts))
    join <$> traverse run eitherDbs

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
  maybeDb <- db dbName
  -- TODO: generalize
  if (isJust maybeDb)
    then do
    ro' <- ro
    let (schema', db') = fromJust maybeDb
    records <- withIterator db' ro' $ \iter -> do
      let mapop        = decodeRecord decoding schema'
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
  mkey <- iterKey iter
  mval <- iterValue iter
  iterNext iter
  return $ (,) <$> (maybeInterrupt mkey) <*> mval
  where maybeInterrupt k = k >>= interruptCondition
        interruptCondition resKey = if resKey <= rangeEnd
                                    then Just resKey
                                    else Nothing

advanceIterator iter (TsKeyRange _ rangeEnd) =
  advanceIterator iter (KeyRange BS.empty (encodeEndTimestamp rangeEnd))

advanceIterator iter (SingleKey singleKey) = do
  mkey <- iterKey iter
  mval <- iterValue iter
  iterNext iter
  return $ (,) <$> (maybeInterrupt mkey) <*> mval
  where maybeInterrupt k = k >>= interruptCondition
        interruptCondition resKey = if resKey == singleKey
                                    then Just resKey
                                    else Nothing

advanceIterator iter (TsSingleKey intKey) = do
  mkey <- iterKey iter
  mval <- iterValue iter
  iterNext iter
  return $ (,) <$> (maybeInterrupt mkey) <*> mval
  where maybeInterrupt k = k >>= interruptCondition
        encodedKey = packWord64 intKey
        interruptCondition resKey = if (BS.take 8 resKey) == encodedKey
                                    then Just resKey
                                    else Nothing


advanceIterator iter _ = do
  mkey <- iterKey iter
  mval <- iterValue iter
  iterNext iter
  return $ (,) <$> mkey <*> mval

setStartPosition :: MonadResource m =>
                    Iterator
                    -> KeyRange
                    -> m ()

setStartPosition iter (OpenEnd startPosition)    = iterSeek iter startPosition
setStartPosition iter (TsOpenEnd startPosition)  =
  setStartPosition iter (OpenEnd (encodeBeginTimestamp startPosition))

setStartPosition iter (SingleKey startPosition)  = iterSeek iter startPosition
setStartPosition iter (TsSingleKey startPosition)  =
  setStartPosition iter (SingleKey (encodeBeginTimestamp startPosition))

setStartPosition iter (KeyRange startPosition _) = iterSeek iter startPosition
setStartPosition iter (TsKeyRange startPosition _) =
  setStartPosition iter (KeyRange (encodeBeginTimestamp startPosition) BS.empty)

setStartPosition iter EntireKeyspace = iterFirst iter



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
    Base.put (ctxChunksDb st) (snd (ctxRwOptions st)) (packWord64 time) BS.empty
  return $ return $ EmptyRes

readChunks :: AppState [Integer]
readChunks = do
  db' <- chunks
  ro' <- ro
  withIterator db' ro' $ \i -> (mapM unpackWord64) <$> (iterFirst i >> iterKeys i)

initializeDbs :: MonadResource m =>
                 String
                 -> DB
                 -> m (DbErrorMonad SchemaMap)
initializeDbs path systemDb = do
  schemas <- withIterator systemDb readOpts $ readSchemas
  traverse (addDbInit path) (sequence schemas)
  where readSchemas i  = (map decodeSchema) <$> (iterFirst i >> iterItems i)

addDbInit :: MonadResource m =>
             String
             -> [(ByteString, DbSchema)]
             -> m SchemaMap
addDbInit path coll =
  Map.fromList <$> mapM initDb coll

  where initDb (dbName, schema) = do
          db <- Base.open (path ++ (C8.unpack dbName)) opts
          return (dbName, (schema, db))

makeRanges :: [Integer]
              -> [KeyRange]
makeRanges (f:s:xs) = (TsKeyRange f s) : makeRanges (s:xs)
makeRanges [a] = [TsOpenEnd a]

parallelScan :: ByteString -> AppState DbResult
parallelScan dbName = do
  chunks <- readChunks
  st     <- get
  let ranges           = makeRanges <$> chunks
      scanChunk r      = scan dbName r (Field "status") (step (Group Count))
      asyncReadChunk i = (execAsyncIO st (scanChunk i)) :: IO (DbErrorMonad DbResult)

  rangeResults <- liftIO $ runRangeQueries ranges asyncReadChunk
  return $ (finalize . mconcat) <$> rangeResults

runRangeQueries :: DbErrorMonad [KeyRange]
                -> (KeyRange -> IO (DbErrorMonad DbResult))
                -> IO (DbErrorMonad [DbResult])
runRangeQueries (Left  err)    _  = return $ Left err
runRangeQueries (Right ranges) op = do
  res <- parallel $ map op ranges
  return $ sequence res

-- asd eitherRanges op = traverse (\i -> mapM op i) eitherRanges


-- So far I've managed to get here: IO (Either DbError [DbErrorMonad DbResult])

-- f (Either DbError [b])
execAsyncIO :: DBContext -> AppState a -> IO (Either DbError a)
execAsyncIO  st op = runResourceT . evalStateT op $ st


--------

data Query = Count
           | Distinct
           | Min
           | Max
           | Group Query
           deriving (Show)

step :: Query -> L.Fold DbResult DbResult
step Count = L.Fold step (CountStep 0) id
  where
    step (CountStep acc) (FieldRes (_, field)) = CountStep $ acc + 1

step (Group Count) = L.Fold step (GroupRes $ Map.empty) id
  where
    step (GroupRes m) (FieldRes (_, field)) =
      GroupRes $! Map.alter inc field m
    inc Nothing = return $! CountStep 0
    inc (Just (CountStep a)) = return $! CountStep (a + 1)

instance Monoid DbResult where
  mempty  = EmptyRes
  mappend (CountStep a) (CountStep b) =
    CountStep $! a + b

  mappend (GroupRes a)  (GroupRes b) =
    GroupRes $! Map.unionWith mappend a b

  mappend a EmptyRes = a
  mappend EmptyRes b = b
  mappend _ _ = ErrorRes NoAggregatorAvailable

finalize :: DbResult -> DbResult
finalize a = a


--


-- createDb
