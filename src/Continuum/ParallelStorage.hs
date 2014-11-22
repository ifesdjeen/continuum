{-# LANGUAGE OverloadedStrings #-}

module Continuum.ParallelStorage
       where

import           Data.Monoid
import           Control.Concurrent.ParallelIO.Global
import           Continuum.Types
import           Continuum.Storage
import           Continuum.Common.Serialization

import           Continuum.Folds                ( appendFold )
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
adjustRanges v@(OpenEnd _)    ranges = concat [[v], (tail ranges)]
adjustRanges v@(SingleKey _)  _      = [v]
adjustRanges v@(KeyRange _ _) []     = [v]
adjustRanges (KeyRange s e)   [(KeyRange s1 e1)] =
  [(KeyRange s s1), (KeyRange s1 e1), (KeyRange e1 e)]

adjustRanges (KeyRange s e) ranges =
  let count                = length ranges
      h@(KeyRange hs _)    = head ranges
      t@(KeyRange _  te)   = last ranges
      middle               = drop 1 (take (count - 1) ranges)
  in concat [[(KeyRange s hs), h], middle, [t, (KeyRange te e)]]

adjustRanges EntireKeyspace ranges = ranges

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

queryStep v@(Group Count) = Fold localStep (GroupRes $ Map.empty) id
  where
    localStep (GroupRes m) (FieldRes (_, field)) =
      GroupRes $! Map.alter inc field m
    localStep _ _ = error ("NOT IMPLEMENTED: " ++ show v)

    inc Nothing = return $! CountStep 0
    inc (Just (CountStep a)) = return $! CountStep (a + 1)
    inc _ = error ("NOT IMPLEMENTED: " ++ show v)

queryStep FetchAll = Fold step (DbResults []) id
  where step (DbResults acc) val = DbResults $ acc ++ [val]

queryStep v = error ("NOT IMPLEMENTED: " ++ show v)

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
