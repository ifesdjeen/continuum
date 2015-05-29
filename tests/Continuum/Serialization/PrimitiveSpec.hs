module Continuum.Serialization.PrimitiveSpec where

import           Continuum.Serialization.Primitive
import           Continuum.Types
import           Test.Hspec

import           Data.Word                   ( Word8, Word16, Word32, Word64 )
import           Data.ByteString             ( ByteString )

import Test.QuickCheck.Monadic
import Test.QuickCheck
import           Control.Monad.Catch    ( MonadMask(..), throwM )

-- wordRoundTrip :: (Integral a, Show a, Eq a, Monad m) =>
--                  (a -> ByteString)
--                  -> (ByteString -> m a)
--                  -> a
--                  -> Property
roundTrip encoder decoder item = monadicIO $ do
  result <- run $ decoder (encoder item)
  assert $ result == item
  -- where res =


spec :: Spec
spec = do

  describe "Primitive" $ do
    it "passes Word8  Round Trip" $ do
      property $ roundTrip (packWord8 :: Word8 -> ByteString) (unpackWord8 :: MonadMask m => ByteString -> m Word8)

    it "passes Word16  Round Trip" $ do
      property $ roundTrip (packWord16 :: Word16 -> ByteString) (unpackWord16 :: MonadMask m => ByteString -> m Word16)

    it "passes Word32  Round Trip" $ do
      property $ roundTrip (packWord32 :: Word32 -> ByteString) (unpackWord32 :: MonadMask m => ByteString -> m Word32)

    it "passes Word64  Round Trip" $ do
      property $ roundTrip (packWord64 :: Word64 -> ByteString) (unpackWord64 :: MonadMask m => ByteString -> m Word64)

    it "passes Float  Round Trip" $ do
      property $ roundTrip packFloat unpackFloat

    it "passes Double  Round Trip" $ do
      property $ roundTrip packDouble unpackDouble
