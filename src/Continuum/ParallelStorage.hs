{-# LANGUAGE OverloadedStrings #-}

module Continuum.ParallelStorage (parallelScan)
       where

import           Data.Monoid
import           Control.Concurrent.ParallelIO.Global
import           Continuum.Types
import           Continuum.Storage
import           Continuum.Common.Serialization

import           Control.Monad.State.Strict     ( get )
import           Control.Monad.State.Strict     ( liftIO, evalStateT )
import           Control.Applicative            ( (<$>) )
import           Data.ByteString                ( ByteString )
import           Control.Foldl                  ( Fold(..) )

import qualified Database.LevelDB.Base          as LDB
import qualified Data.Map.Strict                as Map

-- |Just a demonstration of how parallel scan works
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
execAsyncIO  st op = evalStateT op $ st

-- |Read Chunk ids from the Chunks Database
--
readChunks :: AppState [Integer]
readChunks = do
  db  <- getCtxChunksDb
  ro  <- getReadOptions
  LDB.withIter db ro iter
  where iter i = mapM unpackWord64 <$> (LDB.iterFirst i >> LDB.iterKeys i)

-- |Split chunks into ranges (pretty much partitioning with a step of 1)
--
makeRanges :: [Integer]
              -> [KeyRange]
makeRanges (f:s:xs) = (TsKeyRange f s) : makeRanges (s:xs)
makeRanges [a]      = [TsOpenEnd a]
makeRanges []       = []

-- |
-- | QUERY STEP
-- |

-- |Query Step is given as a Fold to every @Chunk@ processor that's
-- being asynchronously executed. Results of @queryStep@ are then
-- merged with @DbResult@ Monoid and finalized with a @Finalizer@
queryStep :: SelectQuery
             -> Fold DbResult DbResult
queryStep Count = Fold localStep (CountStep 0) id
  where
    localStep (CountStep acc) _ = CountStep $ acc + 1
    localStep _ _ = EmptyRes -- ??

queryStep (Group Count) = Fold localStep (GroupRes $ Map.empty) id
  where
    localStep (GroupRes m) (FieldRes (_, field)) =
      GroupRes $! Map.alter inc field m
    localStep _ _ = error "NOT IMPLEMENTED"

    inc Nothing = return $! CountStep 0
    inc (Just (CountStep a)) = return $! CountStep (a + 1)
    inc _ = error "NOT IMPLEMENTED"

queryStep _ = error "NOT IMPLEMENTED"

-- |
-- | MONOIDS
-- |

-- |@DbResult@ monoid is used to merge instances of @Chunks@ obtained
-- by performing Scan operations in parallel. Results should be piped
-- into @Finalizer@ afterwards.
--
instance Monoid DbResult where
  mempty  = EmptyRes
  mappend (CountStep a) (CountStep b) =
    CountStep $! a + b

  mappend (GroupRes a)  (GroupRes b) =
    GroupRes $! Map.unionWith mappend a b

  mappend a EmptyRes = a
  mappend EmptyRes b = b
  mappend _ _ = ErrorRes NoAggregatorAvailable

-- |
-- | FINALIZERS
-- |

finalize :: DbResult -> DbResult
finalize a = a
