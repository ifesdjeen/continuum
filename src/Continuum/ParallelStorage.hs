{-# LANGUAGE OverloadedStrings #-}

module Continuum.ParallelStorage (parallelScan)
       where

import           Data.Monoid
import           Control.Concurrent.ParallelIO.Global
import           Continuum.Types
import           Continuum.Storage

import           Data.ByteString (ByteString)
import           Control.Monad.State.Strict (get)
import           Control.Applicative ((<$>))
import           Control.Monad.Trans.Resource
import           Control.Monad.State.Strict (liftIO, evalStateT)
import qualified Database.LevelDB.MonadResource as LDB
import           Database.LevelDB.MonadResource (DB,
                                                 Iterator)
import           Continuum.Serialization
import qualified Control.Foldl as Fold
import qualified Data.Map.Strict as Map

parallelScan :: ByteString -> AppState DbResult
parallelScan dbName = do
  chunks <- readChunks
  st     <- get
  let ranges           = makeRanges <$> chunks
      scanChunk r      = scan dbName r (Field "status") (queryStep (Group Count))
      asyncReadChunk i = execAsyncIO st (scanChunk i)

  rangeResults <- liftIO $ parallelRangeScan ranges asyncReadChunk
  return $ (finalize . mconcat) <$> rangeResults

parallelRangeScan :: DbErrorMonad [KeyRange]
                  -> (KeyRange -> IO (DbErrorMonad DbResult))
                  -> IO (DbErrorMonad [DbResult])
parallelRangeScan (Left  err)    _  = return $ Left err
parallelRangeScan (Right ranges) op = do
  res <- parallel $ map op ranges
  return $ sequence res


execAsyncIO :: DBContext -> AppState a -> IO (Either DbError a)
execAsyncIO  st op = runResourceT . evalStateT op $ st

readChunks :: AppState [Integer]
readChunks = do
  db  <- getChunks
  ro  <- getReadOptions
  LDB.withIterator db ro iter
  where iter i = mapM unpackWord64 <$> (LDB.iterFirst i >> LDB.iterKeys i)

makeRanges :: [Integer]
              -> [KeyRange]
makeRanges (f:s:xs) = (TsKeyRange f s) : makeRanges (s:xs)
makeRanges [a]      = [TsOpenEnd a]
makeRanges []       = []

queryStep :: Query -> Fold.Fold DbResult DbResult
queryStep Count = Fold.Fold localStep (CountStep 0) id
  where
    localStep (CountStep acc) (FieldRes (_, _)) = CountStep $ acc + 1

queryStep (Group Count) = Fold.Fold localStep (GroupRes $ Map.empty) id
  where
    localStep (GroupRes m) (FieldRes (_, field)) =
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
