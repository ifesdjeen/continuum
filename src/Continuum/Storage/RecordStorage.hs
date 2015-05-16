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

withField :: (Monoid b) => FieldName -> (Fold DbValue b) -> Fold DbRecord (Maybe b)
withField fieldName (Fold f z0 e) =
  let valueFn      = getValue fieldName
      wrapped z1 x = f <$> z1 <*> (valueFn x)
  in
   Fold wrapped (Just z0) (fmap e)
