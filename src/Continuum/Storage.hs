{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Storage
       (DB, DBContext,
        runApp, putRecord, findTs, findRange, findAll)
       where
import           Continuum.Options
import           Continuum.Serialization
import           Control.Applicative ((<$>))

-- import           Data.Serialize (Serialize, encode, decode)
import           Data.Serialize (encode)
import qualified Data.Map as Map

-- import qualified Database.LevelDB.Base  as Base
-- import           Database.LevelDB.Base (DB, WriteOptions, ReadOptions,
--                                         iterSeek, iterFirst, iterItems, withIter,
--                                         iterNext, iterEntry, iterValid)
-- import           Database.LevelDB.Iterator (Iterator)

import qualified Database.LevelDB.MonadResource  as Base
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterSeek, iterFirst, iterItems,
                                                 withIterator, iterNext, iterEntry,
                                                 iterValid)
---import           Database.LevelDB.Iterator (Iterator)


import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS
import           Control.Monad.State
import           Data.Maybe


import           Control.Monad.Trans.Resource


type AppState a = StateT DBContext (ResourceT IO) a
-- type AppResource a = ResourceT AppState IO a

type RWOptions = (ReadOptions, WriteOptions)

data DBContext = DBContext { ctxDb          :: DB
                             , ctxSchema    :: DbSchema
                             , sequenceNumber :: Integer
                             -- , ctxKeyspace  :: ByteString
                             , ctxRwOptions :: RWOptions
                           }

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
  -- put old {sequenceNumber = (sequenceNumber old) + 1}
  modify (\a -> a {sequenceNumber = (sequenceNumber a) + 1})
  return $ sequenceNumber old

getSequence :: MonadState DBContext m => m Integer
getSequence = gets sequenceNumber

schema :: MonadState DBContext m => m DbSchema
schema = gets ctxSchema

rwOptions :: MonadState DBContext m => m RWOptions
rwOptions = gets ctxRwOptions

ro :: MonadState DBContext m => m ReadOptions
ro = liftM fst rwOptions

wo :: MonadState DBContext m => m WriteOptions
wo = liftM snd rwOptions

tsPlaceholder :: ByteString
tsPlaceholder = encode (DbString "____placeholder" :: DbValue)

putDbValue :: (Integer, Integer) -> DbValue -> AppState ()
putDbValue k v = do
  db' <- db
  wo' <- wo
  Base.put db' wo' (encode k) (encode v)


getDbValue :: DbValue -> AppState (Maybe ByteString)
getDbValue k = do
  db' <- db
  ro' <- ro
  Base.get db' ro' (encode k)

getDbValue2 :: Integer -> AppState (Maybe ByteString)
getDbValue2 k = do
  db' <- db
  ro' <- ro
  Base.get db' ro' (encode (k, -1 :: Integer))

storagePut :: ByteString -> ByteString -> AppState ()
storagePut key value = do
  db' <- db
  wo' <- wo
  Base.put db' wo' key value

putRecord :: DbRecord -> AppState ()
putRecord (DbRecord timestamp record) = do
  sid <- getAndincrementSequence -- :: StateT DBContext IO (Maybe Integer)

  -- Write empty timestamp to be used for iteration, overwrite if needed
  storagePut (encode (timestamp, 0 :: Integer)) tsPlaceholder
  -- Write an actual value to the database

  liftIO $ putStrLn ("=>> Written: " ++ (show (timestamp, sid)))

  storagePut (encode (timestamp, sid)) value

  where value = encode $ fmap snd (Map.assocs record)

findTs :: Integer -> AppState [DbRecord]
findTs timestamp = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIterator db' ro' $ \iter -> do
               iterSeek iter (encode (timestamp, 0 :: Integer))
               -- iterNext iter
               iterWhile iter (isSame timestamp)

  return $ decodeRecords schema' records


isSame :: Integer -> (ByteString, ByteString) -> Bool
isSame end (key, _) = k == end
                      where (k, _) = (decodeKey key)

reachedEnd :: Integer -> (ByteString, ByteString) -> Bool
reachedEnd end (key, _) = k < end
                          where (k, _) = (decodeKey key)
                                -- (encode (end, 0 :: Integer)) /= key

findRange :: Integer -> Integer -> AppState [DbRecord]
findRange begin end = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIterator db' ro' $ \iter -> do
               iterSeek iter (encode (begin, 0 :: Integer))
               iterWhile iter (reachedEnd end)

  return $ decodeRecords schema' records


iterWhile :: (MonadResource m) =>
             Iterator
             -> ((ByteString, ByteString) -> Bool)
             -> m [(ByteString, ByteString)]

iterWhile iter checker = catMaybes <$> go []
  where go acc = do
          valid <- iterValid iter
          next <- iterEntry iter -- liftIO ?

          if not valid
            then return acc
            else if valid && (checker (fromJust next))
                 then do
                   ()  <- iterNext iter
                   go (acc ++ [next])
                 else return (acc ++ [next])


iterUntil :: (MonadResource m) => Iterator -> ((ByteString, ByteString) -> Bool)-> m [(ByteString, ByteString)]
iterUntil iter checker = iterWhile iter (not . checker)


findAll :: AppState [DbRecord]
findAll = do
  db' <- db
  ro' <- ro
  schema' <- schema
  records <- withIterator db' ro' $ \iter -> do
               iterFirst iter
               iterItems iter

  return $ decodeRecords schema' records

runApp :: String -> AppState a -> IO (a)
runApp path actions = do
  runResourceT $ do
    db <- Base.open path opts
    let schema' = makeSchema [("a", DbtInt), ("b", DbtString)]
        ctx = makeContext db schema' (readOpts, writeOpts)
    -- liftResourceT $ (flip evalStateT) ctx actions
    res <- (flip evalStateT) ctx actions
    return $ res

-- compareKeys :: ByteString -> ByteString -> Ordering
-- compareKeys k1 k2 = compare k11 k21
--                     where (k11, k12) = decodeKey k1
--                           (k21, k22) = decodeKey k2
-- customComparator :: Comparator
-- customComparator = Comparator compareKeys
