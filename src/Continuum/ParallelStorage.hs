{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.ParallelStorage
       where

import           Control.Concurrent.ParallelIO.Global
import           Continuum.Context
import           Continuum.Common.Types
import           Continuum.Storage
import           Continuum.Common.Serialization

import           Data.Monoid                    ( mconcat )
import           Continuum.Folds                ( appendFold, queryStep, finalize )
import           Control.Monad.State.Strict     ( get, lift, liftIO, evalStateT )
import           Control.Applicative            ( (<$>) )
import           Control.Foldl                  ( Fold(..) )

import qualified Data.Map.Strict                as Map

import Debug.Trace

parallelScan :: DbName
                -> ScanRange
                -> Decoding
                -> SelectQuery
                -> DbState DbResult
parallelScan dbName scanRange decoding query = do
  chunks  <- readChunks scanRange
  context <- readT

  let ranges           = (adjustRanges scanRange) <$> makeRanges <$> chunks
      scanChunk chunk  = scan context dbName chunk decoding (queryStep query)

  rangeResults <- liftIO $ parallelRangeScan ranges scanChunk
  return $ (finalize . mconcat) <$> rangeResults

parallelRangeScan :: DbErrorMonad [ScanRange]
                  -> (ScanRange -> IO (DbErrorMonad a))
                  -> IO (DbErrorMonad [a])
parallelRangeScan (Left  err)    _  = return $ Left err
parallelRangeScan (Right ranges) op = do
  res <- parallel $ map op ranges
  return $ sequence res

-- execAsyncIO :: DbContext -> IO (DbErrorMonad acc) -> IO (DbErrorMonad a)
-- execAsyncIO  st op = evalStateT op $ st

-- |Split chunks into ranges (pretty much partitioning with a step of 1)
--
makeRanges :: [Integer]
              -> [ScanRange]
makeRanges (f:s:xs) = (KeyRange f s) : makeButFirstRanges (s:xs)
makeRanges [a]      = [OpenEnd a]
makeRanges []       = []
makeRanges _ = error "should never happen"

makeButFirstRanges :: [Integer]
                   -> [ScanRange]
makeButFirstRanges (f:s:xs) = (ButFirst f s) : makeButFirstRanges (s:xs)
makeButFirstRanges [a]      = [OpenEndButFirst a]
makeButFirstRanges []       = []
makeButFirstRanges _ = error "should never happen"

-- dropDuplicates :: [[a]] -> [[a]]
-- dropDuplicates val@[a] = val
-- dropDuplicates (s:xs) = s : (map butfirst xs)
--   where butfirst (x:xs) = xs

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
