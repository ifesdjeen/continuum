{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.ParallelStorage
       where


import           Control.Concurrent.ParallelIO.Global
import           Continuum.Types
import           Continuum.Storage
import           Continuum.Common.Serialization

import           Data.Monoid                    ( mconcat )
import           Continuum.Folds                ( appendFold, queryStep, finalize )
import           Control.Monad.State.Strict     ( get, lift, liftIO, evalStateT )
import           Control.Applicative            ( (<$>) )
import           Control.Foldl                  ( Fold(..) )

import qualified Data.Map.Strict                as Map

-- |Just a demonstration of how parallel scan works
-- example :: ByteString -> AppState DbResult
-- example dbName = do
--   chunks <- readChunks
--   st     <- get
--   let ranges           = makeRanges <$> chunks
--       scanChunk r      = scan dbName r (Field "status") (queryStep (Group Count))
--       asyncReadChunk i = execAsyncIO st (scanChunk i)

--   rangeResults <- liftIO $ parallelRangeScan ranges asyncReadChunk
--   return $ (finalize . mconcat) <$> rangeResults

parallelScan :: DbName
                -> ScanRange
                -> Decoding
                -> SelectQuery
                -> AppState DbResult
parallelScan dbName scanRange decoding query = do
  chunks <- readChunks scanRange
  st     <- get
  let ranges           = (adjustRanges scanRange) <$> makeRanges <$> chunks
      scanChunk chunk  = scan dbName chunk decoding (queryStep query)
      asyncReadChunk i = execAsyncIO st (scanChunk i)

  rangeResults <- liftIO $ parallelRangeScan ranges asyncReadChunk
  return $ (finalize . mconcat) <$> rangeResults

parallelRangeScan :: DbErrorMonad [ScanRange]
                  -> (ScanRange -> IO (DbErrorMonad DbResult))
                  -> IO (DbErrorMonad [DbResult])
parallelRangeScan (Left  err)    _  = return $ Left err
parallelRangeScan (Right ranges) op = do
  res <- parallel $ map op ranges
  return $ sequence res

execAsyncIO :: DBContext -> AppState a -> IO (Either DbError a)
execAsyncIO  st op = evalStateT op $ st

-- |Read Chunk ids from the Chunks Database
--
-- TODO: REWRITE READ CHUNKS TO COMMON SCANNING
readChunks :: ScanRange -> AppState [DbResult]
readChunks scanRange = do
  db     <- getCtxChunksDb
  ro     <- getReadOptions
  chunks <- lift $ scanDb db ro scanRange decodeChunkKey appendFold
  return chunks

-- |Split chunks into ranges (pretty much partitioning with a step of 1)
--
makeRanges :: [DbResult]
              -> [ScanRange]
makeRanges ((KeyRes f):n@(KeyRes s):xs) = (KeyRange f s) : makeRanges (n:xs)
makeRanges [(KeyRes a)]                 = [OpenEnd a]
makeRanges []                           = []
makeRanges _ = error "should never happen"

adjustRanges :: ScanRange -> [ScanRange] -> [ScanRange]
adjustRanges v@(OpenEnd _)      ranges    = concat [[v], (tail ranges)]
adjustRanges v@(SingleKey _)    _         = [v]
adjustRanges v@(KeyRange _ _)   []        = [v]

adjustRanges (KeyRange s e)     [(KeyRange s1 e1)] =
  [(KeyRange s s1), (KeyRange s1 e1), (KeyRange e1 e)]

adjustRanges (KeyRange s e)     ranges =
  let count                = length ranges
      h@(KeyRange hs _)    = head ranges
      t@(KeyRange _  te)   = last ranges
      middle               = drop 1 (take (count - 1) ranges)
  in concat [[(KeyRange s hs), h], middle, [t, (KeyRange te e)]]

adjustRanges EntireKeyspace ranges = ranges
