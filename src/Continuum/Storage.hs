{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord,
        alwaysTrue, scan)
       where

-- import           Debug.Trace
import           Control.Applicative ((<$>), (<*>))
import           Continuum.Types
import           Continuum.Folds
import           Continuum.Options
import           Continuum.Serialization
import qualified Database.LevelDB.MonadResource  as Base
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterKey, iterValue,
                                                 iterSeek, iterFirst, -- iterItems,
                                                 withIterator, iterNext, iterEntry)
import           Data.ByteString                (ByteString)
import qualified Data.ByteString as BS
import           Control.Monad.State.Strict
import           Data.Maybe                     (isJust, fromJust)
import           Control.Monad.Trans.Resource
import qualified Control.Foldl as L
import           Control.Monad.IO.Class    (MonadIO (liftIO))

makeContext :: DB -> DbSchema -> RWOptions -> DBContext
makeContext db' schema' rwOptions' = DBContext {ctxDb = db',
                                                sequenceNumber = 1,
                                                ctxSchema = schema',
                                                ctxRwOptions = rwOptions'}

db :: MonadState DBContext m => m DB
db = gets ctxDb

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
  sid <- getAndincrementSequence
  schema' <- schema
  storagePut $ encodeRecord schema' record sid

alwaysTrue :: a -> b -> Bool
alwaysTrue = \_ _ -> True

runApp :: String -> DbSchema -> AppState a -> IO (a)
runApp path schema' actions = do
  runResourceT $ do
    -- TODO: add indexes
    db' <- Base.open path opts
    let ctx = makeContext db' schema' (readOpts, writeOpts)
    -- liftResourceT $ (flip evalStateT) ctx actions
    res <- (flip evalStateT) ctx actions
    return $ res

-- compareKeys :: ByteString -> ByteString -> Ordering
-- compareKeys k1 k2 = compare k11 k21
--                     where (k11, k12) = decodeKey k1
--                           (k21, k22) = decodeKey k2
-- customComparator :: Comparator
-- customComparator = Comparator compareKeys

liftEither :: (a -> b -> c) -> Either DbError a -> Either DbError b -> Either DbError c
liftEither f (Right a) (Right b) = return $! f a b
liftEither _ (Left a)  _         = Left a
liftEither _ _         (Left b)  = Left b

-- TODO: add batch put operstiaon
-- TODO: add delete operation



scan :: KeyRange -- -> (DbSchema -> (ByteString, ByteString) -> (Either DbError i))
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
        step !acc !x = liftEither foldop acc (mapop x)
    setStartPosition iter keyRange

    -- Funnily enough, all the folloing is the equivalent of same thing, but all three yield
    -- different performance. For now, I'm sticking with leftEither
    -- let step !acc !x = mapop' x >>= \ !i -> strictFmap (\ !acc' -> foldop acc' i) acc
    -- let step !acc !x = liftA2 foldop acc (mapop' x)
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

advanceIterator :: MonadResource m => Iterator -> KeyRange -> m (Maybe (ByteString, ByteString))
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

setStartPosition :: MonadResource m => Iterator -> KeyRange -> m ()

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
