{-# LANGUAGE OverloadedStrings #-}

module Continuum.Repl where

import Continuum.Types
import Continuum.Folds
import Control.Monad.Catch
import System.Directory
import System.IO.Temp

import Data.Default
import Database.LevelDB.Internal ( unsafeClose )
import Database.LevelDB.Base

import Continuum.Serialization.Record
import Continuum.Storage.RecordStorage
import Continuum.Serialization.Schema

import qualified Data.List as L
import Continuum.Storage.ChunkStorage
import Data.ByteString.Char8 ( pack )
import Continuum.Storage.GenericStorage --( entrySlice )
import qualified Continuum.Stream as S

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


a = withTmpDb $ \(Rs db _) -> do
        let schema  = makeSchema [ ("g", DbtString), ("a", DbtLong) ]
            records = [ makeRecord 1 [ ("g", DbString "a"), ("a", DbLong 5) ]
                      , makeRecord 2 [ ("g", DbString "a"), ("a", DbLong 10) ]
                      , makeRecord 3 [ ("g", DbString "a"), ("a", DbLong 15) ]
                      , makeRecord 4 [ ("g", DbString "b"), ("a", DbLong 2) ]
                      , makeRecord 5 [ ("g", DbString "b"), ("a", DbLong 30) ]]
        populate db (map (tupleToBatchOp . (encodeRecord schema 1)) records)
        fieldQuery db AllKeys Record schema (op_groupByField "g" (op_withField "a" op_min))

-- a = withTmpDb $ \(Rs db _) -> do
--   let schema      = makeSchema [ ("a", DbtInt) ]
--       record ts i = encodeRecord schema i $ makeRecord ts [ ("a", DbInt i) ]
--   populate db $ take 500 [uncurry Put (record i i) | i <- [345..]]
--   runQuery db AllKeys (decoded Record schema $ withField "a" $ op_min)

-- fetchPart db range = withIter db def (\iter ->
--                                        S.toList $ S.map (\(_,y) -> y ) $ entrySlice iter range Asc)



-- a = withTmpDb $ \(Rs db _) -> do
--   let schema      = makeSchema [ ("a", DbtInt) ]
--       record ts i = encodeRecord schema i $ makeRecord ts [ ("a", DbInt i) ]
--   populate db $ take 500 [uncurry Put (record i i) | i <- [345..]]
--   runQuery db AllKeys Record schema "a" op_min
  -- runQuery db AllKeys (decoded Record schema $ withField "a" $ op_min)
