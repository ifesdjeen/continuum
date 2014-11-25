{-# LANGUAGE CPP, ForeignFunctionInterface #-}

module Continuum.Common.Primitive where

import           Data.Word              ( Word8, Word16, Word32, Word64 )
import           Data.ByteString        ( ByteString )
import qualified Data.ByteString        as B
import qualified Data.ByteString.Internal as S
import qualified Data.ByteString.Unsafe as Unsafe

import           Control.Applicative    ( (<$>) )
import           Control.Monad.Except   ( forM_, throwError )
import           Foreign                ( Storable, Ptr, sizeOf, poke, peek, castPtr, plusPtr, shiftR, shiftL, alloca )
import           Foreign.C.Types        ( CSize(..) )
import           Foreign.ForeignPtr     ( withForeignPtr )
import           Foreign.Ptr            ( castPtr )

data SerializationError =
  OtherError | NotEnoughInput
  deriving (Show)

-- |
-- | SIZES
-- |

word8Size = sizeOf (undefined :: Word8)
word16Size = sizeOf (undefined :: Word16)
word32Size = sizeOf (undefined :: Word32)
word64Size = sizeOf (undefined :: Word64)

-- |
-- | PACKING
-- |

packWord8      :: Word8 -> ByteString
packWord8 w    = B.pack [w]

packWord16     :: Word16 -> ByteString
packWord16 w16 = writeNBytes word16Size (\i -> fromIntegral $ w16 `shiftR` i)

packWord32     :: Word32 -> ByteString
packWord32 w32 = writeNBytes word32Size (\i -> fromIntegral $ w32 `shiftR` i)

packWord64     :: Word64  -> ByteString
packWord64 w64 = writeNBytes word64Size (\i -> fromIntegral $ w64 `shiftR` i)

packFloat      :: Float   -> ByteString
packFloat      = packWord32 . fromFloat

packDouble     :: Double   -> ByteString
packDouble     = packWord64 . fromFloat

-- |
-- | UNPACKING
-- |

unpackWord8  :: ByteString -> Either SerializationError Word8
unpackWord8 w = Unsafe.unsafeHead <$> safeTake word8Size w

unpackWord16 :: ByteString -> Either SerializationError Word16
unpackWord16 = readNBytes word16Size

unpackWord32 :: ByteString -> Either SerializationError Word32
unpackWord32 = readNBytes word32Size

unpackWord64 :: ByteString -> Either SerializationError Word64
unpackWord64 = readNBytes word64Size

unpackFloat :: ByteString -> Either SerializationError Float
unpackFloat  = readNBytes word32Size

unpackDouble :: ByteString -> Either SerializationError Double
unpackDouble  = readNBytes word64Size

-- |
-- | Helper Functions
-- |

safeTake :: Int -> ByteString -> Either SerializationError ByteString
safeTake i bs =
  if (B.length bs) <= i
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
readNBytes :: Storable a => Int -> ByteString -> Either SerializationError a
readNBytes n bs = do
    (fp,o,_) <- S.toForeignPtr `fmap` (safeTake n bs)
    let k p = peek (castPtr (p `plusPtr` o))
    return $ S.inlinePerformIO (withForeignPtr fp k)
{-# INLINE readNBytes #-}

toFloat :: (Storable w, Storable f) => w -> f
toFloat word = S.inlinePerformIO $ alloca $ \buf -> do
  poke (castPtr buf) word
  peek buf
{-# INLINE toFloat #-}

fromFloat :: (Storable f, Storable w) => f -> w
fromFloat float = S.inlinePerformIO $ alloca $ \buf -> do
  poke (castPtr buf) float
  peek buf
{-# INLINE fromFloat #-}
