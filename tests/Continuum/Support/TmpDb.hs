{-# LANGUAGE OverloadedStrings #-}

module Continuum.Support.TmpDb where

import Continuum.Types
import Control.Monad.Catch
import System.Directory
import System.IO.Temp

import Data.Default
import Database.LevelDB.Internal ( unsafeClose )
import Database.LevelDB.Base     ( open, defaultOptions, createIfMissing, destroy, write )

data Rs = Rs DB FilePath

initDB :: IO Rs
initDB = do
  tmp <- getTemporaryDirectory
  dir <- createTempDirectory tmp "leveldb-streaming-tests"
  db  <- open dir defaultOptions { createIfMissing = True }
  return $ Rs db dir

destroyDB :: Rs -> IO ()
destroyDB (Rs db dir) = unsafeClose db `finally` destroy dir defaultOptions

tupleToBatchOp :: Entry -> BatchOp
tupleToBatchOp (x,y) = Put x y

withTmpDb :: (Rs -> IO c) -> IO c
withTmpDb = bracket initDB destroyDB

populate :: DB -> WriteBatch -> IO ()
populate db = write db def
