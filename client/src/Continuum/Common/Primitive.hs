{-# LANGUAGE CPP, ForeignFunctionInterface #-}

module Continuum.Common.Primitive where

import qualified Data.ByteString           as B
import qualified Data.ByteString.Internal  as S
import qualified Data.ByteString.Unsafe    as Unsafe

import           Continuum.Common.Types ( DbErrorMonad(..), DbError(..) )
import           Data.Word              ( Word8, Word16, Word32, Word64 )
import           Data.ByteString        ( ByteString )
import           Control.Applicative    ( (<$>) )
import           Control.Monad.Except   ( forM_, throwError )
import           Foreign                ( Storable, Ptr, sizeOf, poke, peek, castPtr, plusPtr, shiftR, shiftL, alloca )
import           Foreign.C.Types        ( CSize(..) )
import           Foreign.ForeignPtr     ( withForeignPtr )
import           Foreign.Ptr            ( castPtr )

-- |
-- | SIZES
-- |

word8Size  = sizeOf (undefined :: Word8)
word16Size = sizeOf (undefined :: Word16)
word32Size = sizeOf (undefined :: Word32)
word64Size = sizeOf (undefined :: Word64)

-- |
-- | PACKING
-- |

packWord8      :: (Integral a) => a -> ByteString
packWord8 w    = B.pack [(fromIntegral w)]

packWord16     :: (Integral a) => a -> ByteString
packWord16 w16 = let conv = (fromIntegral w16) :: Word16
                 in  writeNBytes word16Size (\i -> fromIntegral $ conv `shiftR` i)

packWord32     :: (Integral a) => a -> ByteString
packWord32 w32 = let conv = (fromIntegral w32) :: Word32
                 in  writeNBytes word32Size (\i -> fromIntegral $ conv `shiftR` i)

packWord64     :: (Integral a) => a -> ByteString
packWord64 w64 = let conv = (fromIntegral w64) :: Word64
                 in  writeNBytes word64Size (\i -> fromIntegral $ conv `shiftR` i)

packFloat      :: Float   -> ByteString
packFloat      = (packWord32 :: Word32 -> ByteString) . fromFloat

packDouble     :: Double   -> ByteString
packDouble     = (packWord64 :: Word64 -> ByteString) . fromFloat

-- |
-- | UNPACKING
-- |

unpackWord8  :: (Num a) => ByteString -> DbErrorMonad a
unpackWord8 w = fromIntegral <$> Unsafe.unsafeHead <$> safeTake word8Size w

unpackWord16 :: (Num a) => ByteString -> DbErrorMonad a
unpackWord16 w = fromIntegral <$> (readNBytes word16Size w :: DbErrorMonad Word16)

unpackWord32 :: (Num a) => ByteString -> DbErrorMonad a
unpackWord32 w = fromIntegral <$> (readNBytes word32Size w :: DbErrorMonad Word32)

unpackWord64 :: (Num a) => ByteString -> DbErrorMonad a
unpackWord64 w = fromIntegral <$> (readNBytes word64Size w :: DbErrorMonad Word64)

unpackFloat :: ByteString -> DbErrorMonad Float
unpackFloat  = readNBytes word32Size

unpackDouble :: ByteString -> DbErrorMonad Double
unpackDouble  = readNBytes word64Size

-- |
-- | Helper Functions
-- |

safeTake :: Int -> ByteString -> DbErrorMonad ByteString
safeTake i bs =
  if (B.length bs) >= i
  then return $ Unsafe.unsafeTake i bs
  else throwError $ NotEnoughInput
{-# INLINE safeTake #-}

-- |Allocate and Write @n@ bytes in Native host order
writeNBytes :: Int -> (Int -> Word8) -> ByteString
writeNBytes total op = S.unsafeCreate total $ (\p -> writeOne p 0 0)
  where writeOne p i shift = do
          _ <- poke (p `plusPtr` i) (op shift)
          if i == total
            then return ()
            else writeOne p (i + 1) (shift + 8)
{-# INLINE writeNBytes #-}

-- |Read N Bytes in Native host order
readNBytes :: Storable a => Int -> ByteString -> DbErrorMonad a
readNBytes n bs = do
    (fp,o,_) <- S.toForeignPtr `fmap` (safeTake n bs)
    let k p = peek (castPtr (p `plusPtr` o))
    return $ S.inlinePerformIO (withForeignPtr fp k)
{-# INLINE readNBytes #-}

fromFloat :: (Storable f, Storable w) => f -> w
fromFloat float = S.inlinePerformIO $ alloca $ \buf -> do
  poke (castPtr buf) float
  peek buf
{-# INLINE fromFloat #-}
