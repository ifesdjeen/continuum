{-# LANGUAGE OverloadedStrings #-}

module Continuum.Repl where

import Continuum.Types
import Control.Monad.Catch
import System.Directory
import System.IO.Temp

import Data.Default
import Database.LevelDB.Internal ( unsafeClose )
import Database.LevelDB.Base


import Data.ByteString.Char8 ( pack )
import Continuum.Types
import Continuum.Storage.ChunkStorage
import Continuum.Storage.GenericStorage ( entrySlice )
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
  let bounds = addBounds AllTime (take 10 $ [encodeChunkKey i | i <- [0, 10..]])
  populate db $ take 100 [Put (encodeChunkKey i) (pack $ show i) | i <- [1..]]
  mapM (fetchPart db) bounds

fetchPart db range = withIter db def (\iter ->
                                       S.toList $ S.map (\(_,y) -> y ) $ entrySlice iter range Asc)


b = withTmpDb $ \(Rs db _) -> do
  let bounds = addBounds (TimeBetween 15 55) ([encodeChunkKey i | i <- [20,30,40, 50]])
  populate db $ take 100 [Put (encodeChunkKey i) (pack $ show i) | i <- [1..]]
  mapM (fetchPart db) bounds
