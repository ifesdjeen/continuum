{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.ChunkStorageSpec where

import Data.ByteString.Char8 ( pack )
import Continuum.Types
import Continuum.Storage.ChunkStorage
import Continuum.Storage.GenericStorage ( entrySlice )
import Test.Hspec

import Continuum.Support.TmpDb
import Database.LevelDB.Base     ( withIter, write, open, defaultOptions, createIfMissing, destroy )

import Data.Default
import qualified Continuum.Stream as S

spec :: Spec
spec = do

  describe "Time Range Bounds" $ do
    it "Case 1" $ do
      r <- withTmpDb $ \(Rs db _) -> do
        populate db $ take 10 [Put (encodeChunkKey i) (pack $ show i) | i <- [100,200..]]
        -- fetchChunks
      1 `shouldBe` 2

a = withTmpDb $ \(Rs db _) -> do
  let bounds = addBounds AllTime (take 10 $ [encodeChunkKey i | i <- [0, 10..]])
  populate db $ take 100 [Put (encodeChunkKey i) (pack $ show i) | i <- [1..]]
  mapM (fetchPart db) bounds

fetchPart db range = withIter db def (\iter ->
                                       S.toList $ S.map (\(_,y) -> y ) $ entrySlice iter range Asc)
