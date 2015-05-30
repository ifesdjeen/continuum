module Continuum.Storage.SystemStorage where

import Continuum.Types
import Continuum.Serialization.Schema
import Continuum.Storage.GenericStorage

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch    (MonadMask)

import qualified Data.ByteString.Char8  as C8
import qualified Continuum.Stream       as S
import qualified Database.LevelDB.Base  as LDB

fetchDbs :: (MonadMask m, MonadIO m) => DB -> m [(DbName, DbSchema)]
fetchDbs db =
  withIter db def (\iter -> S.toList
                            $ S.mapM decodeSchema
                            $ entrySlice iter AllKeys Asc)

-- |Initialize (open) an instance of an _existing_ database.
--
initializeDb :: String
                -> (DbName, DbSchema)
                -> IO (DbName, (DbSchema, LDB.DB))
initializeDb path (dbName, sch) = do
  ldb <- LDB.open (path ++ "/" ++ (C8.unpack dbName)) def
  return (dbName, (sch, ldb))
