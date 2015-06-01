module Continuum.Storage.RecordStorage where

import Control.Monad.IO.Class ( MonadIO )
import Control.Monad.Catch    ( MonadMask )
import Control.Monad          ( liftM2 )

import Continuum.Folds
import Continuum.Types
import Continuum.Serialization.Record
import Continuum.Storage.GenericStorage

import qualified Continuum.Stream as S
import qualified Database.LevelDB.Base  as LDB

withDecoded :: (Applicative m, MonadIO m, MonadMask m) => Decoding -> DbSchema -> Stream m Entry -> Stream m DbRecord
withDecoded decoding schema = S.mapM (decodeRecord decoding schema)

fieldQuery :: (MonadMask m, MonadIO m, Monoid b)
         => DB -> KeyRange
         -> Decoding -> DbSchema
         -> Fold DbRecord b
         -> m b
fieldQuery db range decoding schema (Fold f z0 e) =
  withIter db def (\iter -> fmap e
                            $ S.foldl f z0
                            $ withDecoded decoding schema
                            $ entrySlice iter range Asc)

putRecord :: (MonadMask m, MonadIO m) => DB -> DbSchema -> Integer -> DbRecord -> m DbResult
putRecord db schema sid record = do
  let (k,v) = encodeRecord schema sid record
  _    <- LDB.put db def k v
  return OK
