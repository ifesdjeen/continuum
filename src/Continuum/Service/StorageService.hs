module Continuum.Service.StorageService where

import Continuum.Types
import Continuum.Storage.GenericStorage

import qualified Data.ByteString.Char8  as C8
import qualified Database.LevelDB.Base  as LDB

-- |Initialize (open) an instance of an _existing_ database.
--
initializeDb :: String
                -> (DbName, DbSchema)
                -> IO (DbName, (DbSchema, LDB.DB))
initializeDb path (dbName, sch) = do
  ldb <- LDB.open (path ++ "/" ++ (C8.unpack dbName)) def
  return (dbName, (sch, ldb))
