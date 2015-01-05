{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Folds where

import           Debug.Trace
import           Continuum.Common.Types

import           Data.List          ( genericLength )
import           Control.Foldl      ( Fold(..), fold )

import           Data.Monoid

import qualified Data.Map.Strict as Map
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
             -> Fold DbRecord StepResult

queryStep (Multi steps) = Fold step [] done
  where step acc val = acc ++ [val]
        done vals    = MultiStep $
                       foldr
                       (\(fieldName, f) m ->
                         Map.insert fieldName (fold (queryStep f) vals) m)
                       Map.empty
                       steps

queryStep (FieldGroup fieldName subquery) = queryStep $ Group (getValue fieldName) subquery
queryStep (TimeGroup  timePeriod subquery) = queryStep $ Group (roundTime timePeriod) subquery

queryStep (Group groupFn subquery) =
  case queryStep subquery of
    (Fold subStep subInit subFinalize) ->
      let
        wrappedSubStep n Nothing  = return $! (subStep subInit n)
        wrappedSubStep n (Just a) = return $! (subStep a n)

        localStep m record =
          Map.alter (wrappedSubStep record) (groupFn record) m

        finalize g = GroupStep $ Map.map subFinalize g
      in
       Fold localStep Map.empty finalize

queryStep Count = Fold localStep (CountStep 0) id
  where
    localStep (CountStep a) e = CountStep $ a + 1

queryStep (Min fieldName) = Fold localStep (MinStep EmptyValue) id
  where
    localStep (MinStep EmptyValue) record = MinStep $ (getValue fieldName record)
    localStep (MinStep a) record          = MinStep $ min a (getValue fieldName record)

queryStep FetchAll = Fold step (ListStep []) id
  where step (ListStep acc) val = ListStep $ acc ++ [val]

queryStep (Avg fieldName) = Fold step (AvgStep []) id
  where step (AvgStep acc) record = AvgStep $ acc ++ [(getValue fieldName record)]

queryStep v = error ("NOT IMPLEMENTED: " ++ show v)

-- |
-- | MONOIDS
-- |

-- |@DbResult@ monoid is used to merge instances of @Chunks@ obtained
-- by performing Scan operations in parallel. Results should be piped
-- into @Finalizer@ afterwards.
--
instance Monoid StepResult where
  mempty  = EmptyStepRes

  mappend (CountStep a) (CountStep b) =
    CountStep $! a + b

  mappend (MinStep a) (MinStep b) =
    MinStep $! min a b

  mappend (AvgStep a) (AvgStep b) =
    AvgStep $! (a ++ b)

  mappend (ListStep a) (ListStep b) =
    ListStep $! (a ++ b)

  mappend (GroupStep a)  (GroupStep b) =
    GroupStep $! Map.unionWith mappend a b

  mappend (MultiStep a)  (MultiStep b) =
    MultiStep $! Map.unionWith mappend a b

  mappend a EmptyStepRes = a
  mappend EmptyStepRes b = b
  mappend _ _ = ErrorStepRes NoAggregatorAvailable

-- |
-- | FINALIZERS
-- |

finalize :: StepResult -> DbResult
finalize (CountStep i) = ValueRes   $ DbInt i
finalize (MinStep i)   = ValueRes   $ i
finalize (AvgStep i)   =
  case withNumbers i average of
    (Left a) -> ErrorRes $ a
    (Right a) -> ValueRes $ DbDouble $ a

  where average nums = trace (show nums) ((sum nums) / (genericLength nums))
  --do
  -- trace (show i) (ValueRes $ DbInt 1)
finalize (ListStep i)  = ListResult $ i
finalize (MultiStep i) = MultiResult $ Map.map finalize i
finalize (GroupStep i) = MapResult $ Map.map finalize i
finalize a = trace (show a) (ErrorRes $ NoStepToResultConvertor)
