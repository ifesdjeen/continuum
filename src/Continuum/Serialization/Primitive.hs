{-# LANGUAGE CPP, ForeignFunctionInterface #-}

module Continuum.Serialization.Primitive
       (
         packWord8
       , packWord16
       , packWord32
       , packWord64
       , packFloat
       , packDouble

       , unpackWord8
       , unpackWord16
       , unpackWord32
       , unpackWord64
       , unpackFloat
       , unpackDouble
       ) where

import qualified Data.ByteString           as B
import qualified Data.ByteString.Internal  as S
import qualified Data.ByteString.Unsafe    as Unsafe

import           Continuum.Types        ( DbErrorMonad, DbError(..) )
import           Data.Word              ( Word8, Word16, Word32, Word64 )
import           Data.ByteString        ( ByteString )
import           Control.Monad.Except   ( throwError )
import           Foreign                ( Storable, sizeOf, poke, peek, castPtr, plusPtr, shiftR, alloca )
import           Foreign.ForeignPtr     ( withForeignPtr )
import           System.IO.Unsafe       ( unsafePerformIO )

-- |
-- | SIZES
-- |
word8Size :: Int
word8Size  = sizeOf (undefined :: Word8)
{-# INLINE word8Size #-}

word16Size :: Int
word16Size = sizeOf (undefined :: Word16)
{-# INLINE word16Size #-}

word32Size :: Int
word32Size = sizeOf (undefined :: Word32)
{-# INLINE word32Size #-}

word64Size :: Int
word64Size = sizeOf (undefined :: Word64)
{-# INLINE word64Size #-}

-- |
-- | PACKING
-- |

packWord8      :: (Integral a) => a -> ByteString
packWord8 w    = B.pack [(fromIntegral w)]
{-# INLINE packWord8 #-}

packWord16     :: (Integral a) => a -> ByteString
packWord16 w16 = let conv = (fromIntegral w16) :: Word16
                 in  writeNBytes word16Size (\i -> fromIntegral $ conv `shiftR` i)
{-# INLINE packWord16 #-}

packWord32     :: (Integral a) => a -> ByteString
packWord32 w32 = let conv = (fromIntegral w32) :: Word32
                 in  writeNBytes word32Size (\i -> fromIntegral $ conv `shiftR` i)
{-# INLINE packWord32 #-}

packWord64     :: (Integral a) => a -> ByteString
packWord64 w64 = let conv = (fromIntegral w64) :: Word64
                 in  writeNBytes word64Size (\i -> fromIntegral $ conv `shiftR` i)
{-# INLINE packWord64 #-}

packFloat      :: Float   -> ByteString
packFloat      = (packWord32 :: Word32 -> ByteString) . fromFloat
{-# INLINE packFloat #-}

packDouble     :: Double   -> ByteString
packDouble     = (packWord64 :: Word64 -> ByteString) . fromFloat
{-# INLINE packDouble #-}

-- |
-- | UNPACKING
-- |

unpackWord8  :: (Num a) => ByteString -> DbErrorMonad a
unpackWord8 w = fixBound <$> Unsafe.unsafeHead <$> safeTake word8Size w
{-# INLINE unpackWord8 #-}

unpackWord16 :: (Num a) => ByteString -> DbErrorMonad a
unpackWord16 w = fixBound <$> (readNBytes word16Size w :: DbErrorMonad Word16)
{-# INLINE unpackWord16 #-}

unpackWord32 :: (Num a) => ByteString -> DbErrorMonad a
unpackWord32 w = fixBound <$> (readNBytes word32Size w :: DbErrorMonad Word32)
{-# INLINE unpackWord32 #-}

unpackWord64 :: (Num a) => ByteString -> DbErrorMonad a
unpackWord64 w = fixBound <$> (readNBytes word64Size w :: DbErrorMonad Word64)
{-# INLINE unpackWord64 #-}

unpackFloat :: ByteString -> DbErrorMonad Float
unpackFloat  = readNBytes word32Size
{-# INLINE unpackFloat #-}

unpackDouble :: ByteString -> DbErrorMonad Double
unpackDouble  = readNBytes word64Size
{-# INLINE unpackDouble #-}

-- |
-- | Helper Functions
-- |

safeTake :: Int -> ByteString -> DbErrorMonad ByteString
safeTake i bs =
  if (B.length bs) >= i
  then return $ Unsafe.unsafeTake i bs
  else throwError $ NotEnoughInput (B.length bs) i
{-# INLINE safeTake #-}

-- |Allocate and Write @n@ bytes in Native host order
writeNBytes :: Int -> (Int -> Word8) -> ByteString
writeNBytes total op = S.unsafeCreate total $ (\p -> writeOne p 0 ((total - 1) * 8))
  where writeOne p i shift = do
          _ <- poke (p `plusPtr` i) (op shift)
          if i == total
            then return ()
            else writeOne p (i + 1) (shift - 8)
{-# INLINE writeNBytes #-}

-- |Read N Bytes in Native host order
readNBytes :: Storable a => Int -> ByteString -> DbErrorMonad a
readNBytes n bs = do
  (fp,o,_) <- S.toForeignPtr `fmap` B.reverse `fmap` (safeTake n bs)
  let k p = peek (castPtr (p `plusPtr` o))
  return $ unsafePerformIO (withForeignPtr fp k)
{-# INLINE readNBytes #-}

fromFloat :: (Storable f, Storable w) => f -> w
fromFloat float = unsafePerformIO $ alloca $ \buf -> do
  poke (castPtr buf) float
  peek buf
{-# INLINE fromFloat #-}

fixBound :: (Integral a, Bounded a, Num b) => a -> b
fixBound i = if i > maxBound `quot` 2
             then (-1) * (fromIntegral $ ((maxBound - i) + 1))
             else fromIntegral i
{-# INLINE fixBound #-}
