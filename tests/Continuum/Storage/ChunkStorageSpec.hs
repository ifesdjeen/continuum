{-# LANGUAGE OverloadedStrings #-}

module Continuum.Storage.ChunkStorageSpec where


import Continuum.Types
import Continuum.Storage.ChunkStorage
import Continuum.Support.TmpDb
import Continuum.Storage.GenericStorage

import Test.Hspec
import Data.Default
import Data.ByteString.Char8  ( pack )
import Control.Monad.IO.Class ( MonadIO )
import Control.Monad.Catch    ( MonadMask )
import qualified Continuum.Stream as S
import qualified Data.List as L

spec :: Spec
spec = do

  describe "Time Range Bounds" $ do
    it "Fetches all the items in the table" $ do
      let bounds = addBounds AllTime (take 10 $ [encodeChunkKey i | i <- [0, 10..]])
          input  = take 100 [(encodeChunkKey i, pack $ show i) | i <- [1..]]
      r <- withTmpDb $ \(Rs db _) -> do
        populate db $ map (uncurry Put) input
        fetchParts db bounds
      r `shouldBe` (map snd input)

    it "Fetches just a range" $ do
      let bounds = addBounds (TimeBetween 15 55) ([encodeChunkKey i | i <- [20,30,40, 50]])
          input  = take 100 [(encodeChunkKey i, pack $ show i) | i <- [1..]]

      r <- withTmpDb $ \(Rs db _) -> do
        populate db $ map (uncurry Put) input
        fetchParts db bounds
      r `shouldBe` [pack $ show i | i <- [(15 :: Integer)..55]]

    it "Fetches just a range (given odd number of ranges)" $ do
      let bounds = addBounds (TimeBetween 15 45) ([encodeChunkKey i | i <- [20,30,40]])
          input  = take 100 [(encodeChunkKey i, pack $ show i) | i <- [1..]]

      r <- withTmpDb $ \(Rs db _) -> do
        populate db $ map (uncurry Put) input
        fetchParts db bounds
      r `shouldBe` [pack $ show i | i <- [(15 :: Integer)..45]]


fetchParts :: (MonadMask m, MonadIO m) => DB -> [KeyRange] -> m [Value]
fetchParts db ranges = L.concat <$> mapM (fetchPart db) ranges

fetchPart :: (MonadMask m, MonadIO m) => DB -> KeyRange -> m [Value]
fetchPart db range = withIter db def (\iter ->
                                       S.toList $ S.map (\(_,y) -> y) $ entrySlice iter range Asc)
