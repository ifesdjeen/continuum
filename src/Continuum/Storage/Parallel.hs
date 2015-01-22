{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Storage.Parallel (parallelScan) where

import           Control.Concurrent.ParallelIO.Local
import           Continuum.Context
import           Continuum.Types
import           Continuum.Storage.Engine

import           Data.List                      ( nub )
import           Data.Monoid                    ( mconcat )
import           Continuum.Folds                ( queryStep, finalize )
import           Control.Monad.State.Strict     ( liftIO )
import           Control.Applicative            ( (<$>) )
import           Continuum.Serialization.Primitive ( packWord64 )
import           Data.ByteString                ( ByteString(..) )

-- import Debug.Trace

parallelScan :: DbName
                -> TimeRange
                -> Decoding
                -> SelectQuery
                -> DbState DbResult
parallelScan dbName timeRange decoding query = do
  chunks  <- readChunks scanRange
  context <- readT

  let ranges           = constructRanges scanRange
                         <$> nub
                         <$> adjustRanges scanRange
                         <$> map packWord64
                         <$> chunks
      -- Maybe makes sense to move chunk to the last place?
      scanChunk chunk  = scan context dbName chunk decoding (queryStep query)

  rangeResults <- liftIO $ parallelRangeScan ranges scanChunk

  return $ (finalizer . mconcat) <$> rangeResults
  where scanRange = toScanRange timeRange
        finalizer        = finalize query

parallelRangeScan :: DbErrorMonad [ScanRange]
                  -> (ScanRange -> IO (DbErrorMonad a))
                  -> IO (DbErrorMonad [a])
parallelRangeScan (Left  err)    _  = return $ Left err
parallelRangeScan (Right ranges) op = do
  res <- withPool 8 $ (\pool -> parallel pool $ map op ranges)
  return $ sequence res

-- execAsyncIO :: DbContext -> IO (DbErrorMonad acc) -> IO (DbErrorMonad a)
-- execAsyncIO  st op = evalStateT op $ st

adjustRanges :: ScanRange -> [ByteString] -> [ByteString]
adjustRanges (OpenEnd a) l          = [a] ++ l
adjustRanges (KeyRange a b) l       = [a] ++ l ++ [b]
-- TODO this is a bug again :/ we have to exclude the first :(
adjustRanges (OpenEndButFirst a) l  = [a] ++ l
adjustRanges (ButFirst a b) l       = [a] ++ l ++ [b]
adjustRanges EntireKeyspace l       = l

constructRanges :: ScanRange -> [ByteString] -> [ScanRange]
constructRanges range []      = [range]
constructRanges range [_]     = [range]
constructRanges range [_, _]  = [range]
constructRanges (OpenEnd _) l = (slide l) ++ [(OpenEnd $ last l)]

constructRanges (KeyRange a b) l       = [KeyRange a (first l)] ++ (slide l) ++ [ButFirst (last l) b]
  where first (s:_) = s

constructRanges EntireKeyspace l       = [(KeyRange (first l) (second l))] ++ (slide (tail l)) ++ [(OpenEndButFirst $ last l)]
  where first (s:_) = s
        second (_:s:_) = s

-- Edge case: when part of the range equals the searched key part
slide :: [ByteString] -> [ScanRange]
slide (f:s:xs) = (ButFirst f s) : slide (s:xs)
slide _ = []

toScanRange :: TimeRange -> ScanRange
toScanRange (TimeAfter start)       = OpenEnd (packWord64 start)
toScanRange (TimeBetween start end) = KeyRange (packWord64 start) (packWord64 end)
toScanRange AllTime                 = EntireKeyspace
