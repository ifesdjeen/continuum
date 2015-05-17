module Continuum.Storage.RecordStorage where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch    (MonadMask)

import Continuum.Folds
import Continuum.Types
import Continuum.Serialization.Record
import Continuum.Storage.GenericStorage

import qualified Continuum.Stream as S

runQuery :: (MonadMask m, MonadIO m, Monoid b) => DB -> KeyRange -> Fold Entry b -> m b
runQuery db range (Fold f z0 e) = withIter db def (\iter -> fmap e $ S.foldl f z0 $ entrySlice iter range Asc)

-- TODO Figure out how MonadMask works
withField :: (Monoid b) => FieldName -> (Fold DbValue b) -> Fold DbRecord (Maybe b)
withField fieldName (Fold f z0 e) =
  let valueFn      = getValue fieldName
      wrapped z1 x = f <$> z1 <*> (valueFn x)
  in
   Fold wrapped (Just z0) (fmap e)

decoded :: (Monoid b) => Decoding -> DbSchema -> (Fold DbRecord b) -> Fold Entry (Maybe b) -- WRONG
decoded decoding schema (Fold f z0 e) =
  let valueFn           = decodeRecord decoding schema
      wrapped z1 x      = f <$> z1 <*> (valueFn x)
      toMaybe (Left _)  = Nothing
      toMaybe (Right a) = Just a
  in
   Fold wrapped (Right z0) (\y -> toMaybe $ fmap e y)
