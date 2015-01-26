{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Storage.C where

import qualified Control.Foldl             as L
import qualified Data.ByteString.Unsafe    as BU
import qualified Data.ByteString           as B

import           Database.LevelDB.Base     ( ReadOptions(..) )
import           Database.LevelDB.C        ( CompareFun, LevelDBPtr, ReadOptionsPtr,
                                                  LevelDBPtr )
import           Database.LevelDB.Internal ( DB(..), mkCReadOpts )
import           Control.Applicative       ( (<*>), (<$>) )
import           Data.ByteString           ( ByteString, packCStringLen )
import           Data.IORef                ( IORef, newIORef, modifyIORef', readIORef )

import           Continuum.Types

import           Foreign
import           Foreign.C.Types
import           Foreign.C.String


-- import Debug.Trace

#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)
#include "continuum.h"

-- |
-- | FFI Decoding
-- |

decodeString :: CString -> CSize -> IO ByteString
decodeString ptr size = packCStringLen (ptr, fromIntegral size)
{-# INLINE decodeString #-}

scan :: DB
        -> ReadOptions
        -> ScanRange
        -> Decoder a
        -> L.Fold a acc
        -> IO (DbErrorMonad acc)

scan (DB dbPtr _) ro scanRange decoder (L.Fold step start done) = do
  cReadOpts <- mkCReadOpts ro

  ref      <- newIORef (Right start)
  appendFn <- makeCStepFun $ makeStepFun ref decoder step
  _        <- doScan dbPtr cReadOpts scanRange appendFn
  result   <- readIORef ref
  return $! done <$> result

doScan :: LevelDBPtr
          -> ReadOptionsPtr
          -> ScanRange
          -> StepFunPtr
          -> IO ()

doScan db ro range appendFn = do
  let sp     = startingPoint range
      cf     = compareFun range
      sf     = skipFirst range
      scanFn = case sp of
        Nothing           -> (\cmp -> c_scan db ro nullPtr (fromIntegral (0 :: Integer)) cmp appendFn sf)
        (Just rangeStart) -> (\cmp ->
                       BU.unsafeUseAsCStringLen rangeStart $ \(start_at_ptr, start_at_len) ->
                       c_scan db ro start_at_ptr (fromIntegral start_at_len) cmp appendFn sf)
  cmp <- mkCmp cf
  scanFn cmp


-- Actually, if we do it this way, we could even introduce some scanning / filtering
-- limit could/should be added to append function.
class DbScanSetup a where
  startingPoint  :: a -> Maybe ByteString
  exitPoint      :: a -> Maybe ByteString
  compareFun     :: a -> CurriedCompareFun
  skipFirst      :: a -> CInt

instance DbScanSetup ScanRange where
  startingPoint (OpenEnd bs)         = Just bs
  startingPoint (ButFirst bs _)      = Just bs
  startingPoint (OpenEndButFirst bs) = Just bs
  startingPoint EntireKeyspace       = Nothing
  startingPoint (KeyRange bs _)      = Just bs
  startingPoint (Prefixed prefix subscan)   =
    Just $
    case (startingPoint subscan) of
      Nothing -> prefix
      (Just bs) -> B.concat [prefix, bs]

  exitPoint (OpenEnd _)         = Nothing
  exitPoint (ButFirst _ end)    = Just end
  exitPoint (OpenEndButFirst _) = Nothing
  exitPoint EntireKeyspace      = Nothing
  exitPoint (KeyRange _ end)    = Just end
  exitPoint (Prefixed _ _)      = undefined

  compareFun (Prefixed prefix subscan) = case (exitPoint subscan) of
    Nothing         -> prefixEqualCmp prefix
    (Just rangeEnd) -> bitwiseCompareHs (B.concat [prefix, rangeEnd])

  compareFun range                  = case (exitPoint range) of
    Nothing         -> constantlyTrue
    (Just rangeEnd) -> bitwiseCompareHs rangeEnd

  skipFirst (OpenEnd _)           = fromIntegral (0 :: Integer)
  skipFirst (OpenEndButFirst _)   = fromIntegral (1 :: Integer)
  skipFirst EntireKeyspace        = fromIntegral (0 :: Integer)
  skipFirst (KeyRange _ _)        = fromIntegral (0 :: Integer)
  skipFirst (ButFirst _ _)        = fromIntegral (1 :: Integer)
  skipFirst (Prefixed _ subrange) = skipFirst subrange

  -- startingPoint (Prefixed prefix subscan)   = Just $

-- |
-- | Foreign imports
-- |

foreign import ccall safe "continuum.h scan"
  c_scan :: LevelDBPtr
            -> ReadOptionsPtr
            -> CString
            -> CSize
            -> FunPtr CurriedCompareFun
            -> FunPtr StepFun
            -> CInt
            -> IO ()

foreign import ccall safe "continuum.h bitwise_compare"
  ___bitwise_compare :: CompareFun

foreign import ccall safe "continuum.h constantly_true"
  constantlyTrue :: CurriedCompareFun

type HsCompareFun = CString -> CSize -> CString -> CSize -> IO CInt

bitwiseCompare :: HsCompareFun
bitwiseCompare = ___bitwise_compare nullPtr

-- |
-- | Comparator functions
-- |

bitwiseCompareHs :: ByteString -> CurriedCompareFun
bitwiseCompareHs end ptr size = do
  current <- BU.unsafePackCStringLen (ptr, (fromIntegral size))
  return $ orderToInt $ compare (BU.unsafeTake 8 current) (BU.unsafeTake 8 end)

prefixEqualCmp :: ByteString -> CurriedCompareFun
prefixEqualCmp prefix ptr size = do
  current <- BU.unsafePackCStringLen (ptr, (fromIntegral size))
  if ((B.take (B.length prefix) current) == prefix)
    then return 0
    else return 1

orderToInt :: Ordering -> CInt
orderToInt LT = -1
orderToInt GT = 1
orderToInt EQ = 0

type StepFun = CString -> CSize -> CString -> CSize -> IO ()

makeStepFun :: IORef (DbErrorMonad x) -> Decoder a -> (x -> a -> x) -> StepFun
makeStepFun ref decoder step keyPtr keyLen valPtr valLen = do
  key <- decodeString keyPtr keyLen
  val <- decodeString valPtr valLen
  modifyIORef' ref (\prev -> step <$> prev <*> (decoder (key, val))) -- Do we need forcing seq here?
{-# INLINE makeStepFun #-}

type StepFunPtr = FunPtr StepFun

foreign import ccall safe "wrapper" makeCStepFun :: StepFun -> IO StepFunPtr

type CurriedCompareFun = CString -> CSize -> IO CInt

foreign import ccall "wrapper" mkCmp :: CurriedCompareFun -> IO (FunPtr CurriedCompareFun)
