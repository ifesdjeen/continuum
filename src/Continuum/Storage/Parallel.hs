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

-- import Debug.Trace

parallelScan :: DbName
                -> ScanRange
                -> Decoding
                -> SelectQuery
                -> DbState DbResult
parallelScan dbName scanRange decoding query = do
  chunks  <- readChunks scanRange
  context <- readT

  let ranges           = (constructRanges scanRange) <$> nub <$> (adjustRanges scanRange) <$> chunks
      scanChunk chunk  = scan context dbName chunk decoding (queryStep query)

  rangeResults <- liftIO $ parallelRangeScan ranges scanChunk

  return $ (finalize . mconcat) <$> rangeResults

parallelRangeScan :: DbErrorMonad [ScanRange]
                  -> (ScanRange -> IO (DbErrorMonad a))
                  -> IO (DbErrorMonad [a])
parallelRangeScan (Left  err)    _  = return $ Left err
parallelRangeScan (Right ranges) op = do
  print $ ranges
  res <- withPool 8 $ (\pool -> parallel pool $ map op ranges)
  return $ sequence res

-- execAsyncIO :: DbContext -> IO (DbErrorMonad acc) -> IO (DbErrorMonad a)
-- execAsyncIO  st op = evalStateT op $ st

adjustRanges :: ScanRange -> [Integer] -> [Integer]
adjustRanges (OpenEnd a) l          = [a] ++ l
adjustRanges (KeyRange a b) l       = [a] ++ l ++ [b]
-- TODO this is a bug again :/ we have to exclude the first :(
adjustRanges (OpenEndButFirst a) l  = [a] ++ l
adjustRanges (ButFirst a b) l       = [a] ++ l ++ [b]
adjustRanges (ButLast a b) l        = [a] ++ l ++ [b]
adjustRanges (ExclusiveRange a b) l = [a] ++ l ++ [b]
adjustRanges EntireKeyspace l       = l

constructRanges :: ScanRange -> [Integer] -> [ScanRange]
constructRanges range []      = [range]
constructRanges range [_]     = [range]
constructRanges range [_, _]  = [range]
constructRanges (OpenEnd _) l = (slide l) ++ [(OpenEnd $ last l)]

constructRanges (KeyRange a b) l       = [KeyRange a (first l)] ++ (slide l) ++ [ButFirst (last l) b]
  where first (s:_) = s

-- constructRanges (OpenEndButFirst _) l  = [a] ++ l
-- constructRanges (ButFirst _ _) l       = [a] ++ l ++ [b]
-- constructRanges (ButLast _ _) l        = [a] ++ l ++ [b]
-- constructRanges (ExclusiveRange _ _) l = [a] ++ l ++ [b]
constructRanges EntireKeyspace l       = [(KeyRange (first l) (second l))] ++ (slide (tail l)) ++ [(OpenEndButFirst $ last l)]
  where first (s:_) = s
        second (_:s:_) = s

-- Edge case: when part of the range equals the searched key part
slide :: [Integer] -> [ScanRange]
slide (f:s:xs) = (ButFirst f s) : slide (s:xs)
slide _ = []
