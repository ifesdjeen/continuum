module Continuum.Storage.SystemStorage where

import Continuum.Types
import Continuum.Serialization.Schema
import Continuum.Storage.GenericStorage

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch    (MonadMask)

import qualified Continuum.Stream       as S
import qualified Database.LevelDB.Base  as LDB

fetchDbs :: (MonadMask m, MonadIO m) => DB -> m [(DbName, DbSchema)]
fetchDbs db =
  withIter db def (\iter -> S.toList
                            $ S.mapM decodeSchema
                            $ entrySlice iter AllKeys Asc)

-- |Create a database if it does not yet exist.
-- Database is returned in an open state, ready for writes.
--
-- createDatabase :: DbName
--                   -> DbSchema
--                   -> DbState DbResult
createDatabase :: (MonadMask m, MonadIO m) => DB -> DbName -> DbSchema -> m DbResult
createDatabase sysDb dbName sch = do
  _    <- LDB.put sysDb def dbName (encodeSchema sch)
  return OK
