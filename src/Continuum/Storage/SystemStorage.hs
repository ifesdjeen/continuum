module Continuum.Storage.SystemStorage where

import Continuum.Types
import Continuum.Serialization.Schema
import Continuum.Storage.GenericStorage
-- import Data.ByteString
import Control.Monad.IO.Class ( MonadIO, liftIO )
import Control.Monad.Catch    ( MonadMask )
import System.Process         ( system )
import qualified Data.ByteString.Char8  as BS
import qualified Continuum.Stream       as S
import qualified Database.LevelDB.Base  as LDB

fetchDbs :: (MonadMask m, MonadIO m) => DB -> m [(DbName, DbSchema)]
fetchDbs db =
  withIter db def (\iter -> S.toList
                            $ S.mapM decodeSchema
                            $ entrySlice iter AllKeys Asc)

-- |Create a database if it does not yet exist.
-- Database is returned in an open state, ready for writes.
createDatabase :: (MonadMask m, MonadIO m) => DB -> DbName -> DbSchema -> m DbResult
createDatabase sysDb dbName sch = do
  _    <- LDB.put sysDb def dbName (encodeSchema sch)
  return OK

withDb :: (MonadMask m, MonadIO m) => String -> DbName -> (DB -> m a) -> m a
withDb path dbName action = do
  _  <- liftIO $ system ("mkdir " ++ path)
  LDB.withDB (path ++ "/" ++ (BS.unpack dbName)) def action
