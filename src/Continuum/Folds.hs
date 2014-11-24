{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Folds where

import           Continuum.Types
import qualified Data.Map.Strict as Map
import           Control.Foldl      ( Fold(..) )
import           Data.Monoid

appendFold :: Fold a [a]
appendFold = Fold step [] id
  where step acc val = acc ++ [val]

-- |
-- | QUERY STEP
-- |

-- |Query Step is given as a Fold to every @Chunk@ processor that's
-- being asynchronously executed. Results of @queryStep@ are then
-- merged with @DbResult@ Monoid and finalized with a @Finalizer@
queryStep :: SelectQuery
             -- TODO: Maybe makes sense to add error result here? O_O
             -> Fold DbResult DbResult

queryStep v@(Group fieldName subquery) =
  case queryStep subquery of
    (Fold subLocalStep subInit subFinalize) ->
      let
        wrappedSubLocalStep _ Nothing  = return $! subInit
        wrappedSubLocalStep n (Just a) = return $! (subLocalStep a n)

        localStep m v@(RecordRes record) =
          Map.alter (wrappedSubLocalStep v) (getValue fieldName record) m

        finalize g = GroupRes $ Map.map subFinalize g
      in
       Fold localStep Map.empty finalize

queryStep Count = Fold localStep (CountStep 0) id
  where
    -- TODO: consider turning DbResult into Functor
    localStep (CountStep a) _ = CountStep $ a + 1

queryStep FetchAll = Fold step (DbResults []) id
  where step (DbResults acc) val = DbResults $ acc ++ [val]
          -- if ((length acc) > 100)
          -- then DbResults $ acc
          -- else

  -- where step (DbResults acc) val = DbResults $ acc ++ [val]

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
