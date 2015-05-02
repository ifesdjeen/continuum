module Continuum.Serialization.PrimitiveSpec where

import           Continuum.Serialization.Primitive
import           Continuum.Types
import           Test.Hspec

import           Data.Word                   ( Word8, Word16, Word32, Word64 )
import           Data.ByteString             ( ByteString )

import qualified Test.QuickCheck as QC


wordRoundTrip :: (Integral a, Show a, Eq a) =>
                 (a -> ByteString)
                 -> (ByteString -> DbErrorMonad a)
                 -> a
                 -> Bool
wordRoundTrip encoder decoder item = res == Right item
  where res = decoder (encoder item)


spec :: Spec
spec = do

  describe "Primitive" $ do
    it "passes Word8  Round Trip" $ do
      QC.property $ wordRoundTrip (packWord8 :: Word8 -> ByteString) (unpackWord8 :: ByteString -> DbErrorMonad Word8)

    it "passes Word16  Round Trip" $ do
      QC.property $ wordRoundTrip (packWord16 :: Word16 -> ByteString) (unpackWord16 :: ByteString -> DbErrorMonad Word16)

    it "passes Word32  Round Trip" $ do
      QC.property $ wordRoundTrip (packWord32 :: Word32 -> ByteString) (unpackWord32 :: ByteString -> DbErrorMonad Word32)

    it "passes Word64  Round Trip" $ do
      QC.property $ wordRoundTrip (packWord64 :: Word64 -> ByteString) (unpackWord64 :: ByteString -> DbErrorMonad Word64)

    it "passes Float  Round Trip" $ do
      QC.property $ \x -> (Right x) == (unpackFloat $ packFloat x)

    it "passes Double  Round Trip" $ do
      QC.property $ \x -> (Right x) == (unpackDouble $ packDouble x)
