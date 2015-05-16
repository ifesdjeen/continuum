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

-- withField :: (Monoid b) => (Fold ) -> (Fold Entry b)
-- withField = undefined
