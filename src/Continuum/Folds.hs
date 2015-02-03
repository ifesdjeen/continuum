{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}

module Continuum.Folds where

import           Debug.Trace
import           Continuum.Types
-- import           Continuum.Helpers

import           Data.List           ( foldl', sort, genericLength )
import           Control.Foldl       ( Fold(..), fold )
import           Control.Applicative ( (<*>), (<$>) )

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

queryStep (TimeFieldGroup fieldName timePeriod subquery) =
  queryStep $ Group (\i -> DbList [getValue fieldName i, roundTime timePeriod i]) subquery

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

        done g = GroupStep $ Map.map subFinalize g
      in
       Fold localStep Map.empty done

queryStep Count = Fold localStep (CountStep 0) id
  where
    localStep (CountStep a) e = let next = a + 1
                                in seq next (CountStep $ next)

queryStep (Min fieldName) = Fold localStep (MinStep EmptyValue) id
  where
    localStep (MinStep EmptyValue) record = MinStep $ (getValue fieldName record)
    localStep (MinStep a) record          = MinStep $ min a (getValue fieldName record)

queryStep (Max fieldName) = Fold localStep (MaxStep EmptyValue) id
  where
    localStep (MaxStep EmptyValue) record = MaxStep $ (getValue fieldName record)
    localStep (MaxStep a) record          = MaxStep $ max a (getValue fieldName record)

queryStep (Median field)  = Fold step (MedianStep []) id
  where step (MedianStep acc) val = MedianStep ((getValue field val) : acc)

queryStep FetchAll = Fold step (ListStep []) id
  where step (ListStep acc) val = ListStep $ acc ++ [val]

queryStep (Mean fieldName) = Fold step start id
  where
    start                            = MeanStep (Right 0) 0
    getNumber f r                    = toNumber $ getValue f r
    step (MeanStep sum count) record =
      MeanStep ((+) <$> (getNumber fieldName record) <*> sum) (count + 1)


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

  mappend (MaxStep a) (MaxStep b) =
    MaxStep $! max a b

  mappend (MeanStep sum1 total1) (MeanStep sum2 total2) =
    let sum    = (+) <$> sum1 <*> sum2
        total  = total1 + total2
    in MeanStep sum total

  mappend (ListStep a) (ListStep b) =
    ListStep $! (a ++ b)

  mappend (MedianStep a) (MedianStep b) =
    MedianStep $! (a ++ b)

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

finalize :: SelectQuery -> StepResult -> DbResult
finalize Count    (CountStep i)        = ValueRes   $ DbInt i
finalize (Min _)  (MinStep i)          = ValueRes   $ i
finalize (Max _)  (MaxStep i)          = ValueRes   $ i
finalize (Mean _) (MeanStep msum nums) =
  case msum of
    (Left a) -> ErrorRes $ a
    (Right sum) -> ValueRes $ DbDouble $ sum / (fromIntegral nums)

finalize (Median _) (MedianStep vals) | odd n = ValueRes  $ head $ drop (n `div` 2) vals'
                                      | even n = numToResult $ (flip withNumbers) mean vals'
  where i      = (length vals' `div` 2) - 1
        vals'  = sort vals
        n      = length vals
        mean x = fst $ foldl' (\(!m, !n) x -> (m+(x-m)/(n+1),n+1)) (0,0) x

finalize FetchAll (ListStep i)  = ListResult $ i
finalize (Multi subqueries) (MultiStep result) = MultiResult $ Map.fromList $ map f subqueries
  where f (field, subquery) = (field,) $
          case (Map.lookup field result) of
            (Just r) -> finalize subquery r
            Nothing  -> ErrorRes OtherError

  -- MultiResult $ map finalize i

finalize (Group _ q)            (GroupStep i) = MapResult $ Map.map (finalize q) i
finalize (FieldGroup _ q)       (GroupStep i) = MapResult $ Map.map (finalize q) i
finalize (TimeGroup _ q)        (GroupStep i) = MapResult $ Map.map (finalize q) i
finalize (TimeFieldGroup _ _ q) (GroupStep i) = MapResult $ Map.map (finalize q) i

finalize a b = trace ((show a) ++ " ::: " ++ (show b)) (ErrorRes $ NoStepToResultConvertor)
