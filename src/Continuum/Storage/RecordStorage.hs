module Continuum.Storage.RecordStorage where

import Control.Monad.IO.Class ( MonadIO )
import Control.Monad.Catch    ( MonadMask )
import Control.Monad          ( liftM2 )

import Continuum.Folds
import Continuum.Types
import Continuum.Serialization.Record
import Continuum.Storage.GenericStorage

import qualified Continuum.Stream as S

withDecoded :: (Applicative m, MonadIO m) => Decoding -> DbSchema -> Stream m Entry -> Stream m (DbErrorMonad DbRecord)
withDecoded decoding schema = S.map (decodeRecord decoding schema)

-- withField :: (Applicative m, MonadIO m) => FieldName -> Stream m (DbErrorMonad DbRecord) -> Stream m (DbErrorMonad DbValue)
-- withField field = S.map (>>= \i -> getValue' field i)

fieldQuery :: (MonadMask m, MonadIO m, Monoid b)
         => DB -> KeyRange
         -> Decoding -> DbSchema
         -> Fold DbRecord b
         -> m (DbErrorMonad b)
fieldQuery db range decoding schema (Fold f z0 e) =
  withIter db def (\iter -> fmap (fmap e)
                            $ S.foldl (\z1 x -> f <$> z1 <*> x) (Right z0)
                            $ withDecoded decoding schema
                            $ entrySlice iter range Asc)
