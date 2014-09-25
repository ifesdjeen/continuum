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

import           Continuum.Folds
import           Continuum.Options
import           Continuum.Serialization
import           Continuum.Types
import           Control.Applicative ((<$>), (<*>))
import           Control.Concurrent
import           Control.Concurrent.ParallelIO.Global
import           Data.Either (rights)
import qualified Data.Map.Strict as Map
import           Data.Monoid
import qualified Database.LevelDB.MonadResource  as Base
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterKey, iterValue,
                                                 iterSeek, iterFirst, -- iterItems,
                                                 withIterator, iterNext, iterKeys)
import           Data.ByteString                (ByteString)
import qualified Data.ByteString as BS
import           Control.Monad.State.Strict
import           Data.Maybe                     (isJust, fromJust, catMaybes)
import           Control.Monad.Trans.Resource
import qualified Control.Foldl as L

makeContext :: DB -> DB -> DbSchema -> RWOptions -> DBContext
makeContext mainDb chunksDb dbSchema rwOptions' =
  DBContext {ctxDb          = mainDb,
             ctxChunksDb    = chunksDb,
             sequenceNumber = 1,
             lastSnapshot   = 1,
             ctxSchema      = dbSchema,
             ctxRwOptions   = rwOptions'}


db :: MonadState DBContext m => m DB
db = gets ctxDb

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

storagePut :: (ByteString, ByteString) -> AppState ()
storagePut (key, value) = do
  db' <- db
  wo' <- wo
  Base.put db' wo' key value

putRecord :: DbRecord -> AppState ()
putRecord record = do
  sid     <- getAndincrementSequence
  ()      <- maybeWriteChunk sid record
  schema' <- schema
  storagePut $ encodeRecord schema' record sid

alwaysTrue :: a -> b -> Bool
alwaysTrue = \_ _ -> True

runApp :: String -> DbSchema -> AppState a -> IO (a)
runApp path dbSchema actions = do
  runResourceT $ do
    -- TODO: add indexes
    mainDb   <- Base.open (path ++ "/mainDb") opts
    chunksDb <- Base.open (path ++ "/chunksDb") opts
    let ctx = makeContext mainDb chunksDb dbSchema (readOpts, writeOpts)
    -- liftResourceT $ (flip evalStateT) ctx actions
    evalStateT actions ctx

liftEither :: (a -> b -> c)
              -> Either DbError a
              -> Either DbError b
              -> Either DbError c
liftEither f (Right a) (Right b) = return $! f a b
liftEither _ (Left a)  _         = Left a
liftEither _ _         (Left b)  = Left b

-- TODO: add batch put operstiaon
-- TODO: add delete operation

scan :: KeyRange
        -> Decoding
        -> L.Fold DbResult acc
        -> AppState (Either DbError acc)

scan keyRange decoding (L.Fold foldop acc done) = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIterator db' ro' $ \iter -> do
    let mapop        = decodeRecord decoding schema'
        getNext      = advanceIterator iter keyRange
        step !a !x   = liftEither foldop a (mapop x)
    setStartPosition iter keyRange
    scanStep getNext step (Right acc)
  return $! fmap done records -- (records >>= (\x -> return $ done x))


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

maybeWriteChunk :: Integer -> DbRecord -> AppState ()
maybeWriteChunk sid (DbRecord time _) = do
  st <- get
  when (sid >= (lastSnapshot st) + snapshotAfter || sid == 1) $ do
    modify (\a -> a {lastSnapshot = sid})
    Base.put (ctxChunksDb st) (snd (ctxRwOptions st)) (packWord64 time) BS.empty

readChunks :: AppState [Integer]
readChunks = do
  db' <- chunks
  ro' <- ro
  withIterator db' ro' $ \i -> (map unpackWord64) <$> (iterFirst i >> iterKeys i)

makeRanges :: [Integer]
              -> [KeyRange]
makeRanges (f:s:xs) = (TsKeyRange f s) : makeRanges (s:xs)
makeRanges [a] = [TsOpenEnd a]

parallelScan :: AppState DbResult
parallelScan = do
  c <- makeRanges <$> readChunks
  st <- get
  liftIO $ print c
  -- So that fold is kind of incorrect. What we need is a merge step.
  -- Although to make a merge step more usable and correct, we have
  -- to collect the data in a different way.
  let readChunk r = scan r (Field "status") (step (Group Count))

  res <- liftIO $ parallel $ fmap (\i -> execAsyncIO st (readChunk i)) c

  return $ finalize $ mconcat (rights res)

execAsyncIO :: DBContext -> AppState a -> IO a
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
  mappend _ _ = ErrorRes

finalize :: DbResult -> DbResult
finalize a = a
