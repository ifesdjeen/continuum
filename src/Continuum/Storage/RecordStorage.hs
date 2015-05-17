module Continuum.Storage.RecordStorage where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch    (MonadMask)
import Control.Monad     ( liftM2 )

import Continuum.Folds
import Continuum.Types
import Continuum.Serialization.Record
import Continuum.Storage.GenericStorage

import qualified Continuum.Stream as S

withDecoded :: (Applicative m, MonadIO m) => Decoding -> DbSchema -> Stream m Entry -> Stream m (DbErrorMonad DbRecord)
withDecoded decoding schema = S.map (decodeRecord decoding schema)

withField :: (Applicative m, MonadIO m) => FieldName -> Stream m (DbErrorMonad DbRecord) -> Stream m (DbErrorMonad DbValue)
withField field = S.map (>>= \i -> getValue' field i)

runQuery :: (MonadMask m, MonadIO m, Monoid b)
         => DB -> KeyRange
         -> Decoding -> DbSchema
         -> FieldName
         -> Fold DbValue b -> m (DbErrorMonad b)
runQuery db range decoding schema field (Fold f z0 e)=
  withIter db def (\iter -> fmap (fmap e)
                                                            $ S.foldl (\z1 x -> f <$> z1 <*> x) (Right z0)
                                                            $ withField field
                                                            $ withDecoded decoding schema
                                                            $ entrySlice iter range Asc)

-- runQuery :: (MonadMask m, MonadIO m, Monoid b) => DB -> KeyRange -> Fold Entry b -> m b
-- runQuery db range (Fold f z0 e) = withIter db def (\iter -> fmap e $ S.foldl f z0 $ entrySlice iter range Asc)


-- TODO Figure out how MonadMask works
-- withField :: (Monoid b) => FieldName -> (Fold DbValue b) -> FoldM DbRecord DbErrorMonad b
-- withField fieldName (Fold f z0 e) =
--   let valueFn      = getValue' fieldName
--       wrapped z1 x = f z1 <$> (valueFn x)
--   in
--    FoldM wrapped z0 (fmap e)

-- decoded :: (Monoid b) => Decoding -> DbSchema -> (Fold DbRecord b) -> Fold Entry (DbErrorMonad b) -- WRONG
-- decoded decoding schema (Fold f z0 e) =
--   let valueFn           = decodeRecord decoding schema
--       wrapped z1 x      = f <$> z1 <*> (valueFn x)
--   in
--    Fold wrapped (Right z0) (\y -> fmap e y)
