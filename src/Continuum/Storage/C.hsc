{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Storage.C where

import qualified Data.ByteString.Unsafe    as BU

import           Database.LevelDB.Base     ( ReadOptions(..) )
import           Database.LevelDB.C        ( CompareFun, LevelDBPtr, ReadOptionsPtr,
                                                  LevelDBPtr )
import           Database.LevelDB.Internal ( DB(..), mkCReadOpts )
import           Control.Applicative       ( (<*>), (<$>) )
import           Data.ByteString           ( ByteString, packCStringLen )
import           Data.IORef                ( IORef, newIORef, modifyIORef', readIORef )

import           Continuum.Serialization.Primitive
import           Continuum.Types

import           Foreign
import           Foreign.C.Types
import           Foreign.C.String
import           Foreign.Ptr

import qualified Control.Foldl                  as L
import Debug.Trace

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
  let sp = startingPoint range
      ec = exitCondition range
      sf = skipFirst range

  compareFun <- mkCmp ec

  case sp of
    Nothing   -> c_scan db ro nullPtr (fromIntegral 0) compareFun appendFn sf
    (Just bs) ->
      BU.unsafeUseAsCStringLen bs $ \(start_at_ptr, start_at_len) ->
      c_scan db ro start_at_ptr (fromIntegral start_at_len) compareFun appendFn sf

-- Actually, if we do it this way, we could even introduce some scanning / filtering
class DbScanSetup a where
  startingPoint  :: a -> Maybe ByteString
  exitCondition  :: a -> CurriedCompareFun
  skipFirst      :: a -> CInt

instance DbScanSetup ScanRange where
  startingPoint (OpenEnd bs)         = Just bs
  startingPoint (OpenEndButFirst bs) = Just bs
  startingPoint EntireKeyspace       = Nothing
  startingPoint (KeyRange bs _)      = Just bs
  startingPoint (ButFirst bs _)      = Just bs

  exitCondition (OpenEnd bs)          = constantlyTrue
  exitCondition (OpenEndButFirst bs)  = constantlyTrue
  exitCondition EntireKeyspace        = constantlyTrue
  exitCondition (ButFirst _ rangeEnd) =
    (\p1 l1 ->
      BU.unsafeUseAsCStringLen rangeEnd $ \(end_at_ptr, end_at_len) -> do
        bitwiseCompare p1 l1 end_at_ptr (fromIntegral end_at_len))
  exitCondition (KeyRange _ rangeEnd) =
    (\p1 l1 ->
      BU.unsafeUseAsCStringLen rangeEnd $ \(end_at_ptr, end_at_len) -> do
        bitwiseCompare p1 l1 end_at_ptr (fromIntegral end_at_len))

  skipFirst (OpenEnd _)         = fromIntegral 0
  skipFirst (OpenEndButFirst _) = fromIntegral 1
  skipFirst EntireKeyspace      = fromIntegral 0
  skipFirst (KeyRange _ _)      = fromIntegral 0
  skipFirst (ButFirst _ _)      = fromIntegral 1
  -- exitCondition (ButFirst bs _)      = Just bs

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

bitwiseCompare :: CString -> CSize -> CString -> CSize -> IO CInt
bitwiseCompare = ___bitwise_compare nullPtr

-- |
-- | Comparator functions
-- |

-- prefix_eq_comparator :: CompareFun
-- prefix_eq_comparator =
--   mkCompareFun $ (\a b ->
--                    let a' = BU.unsafeTake 8 a
--                        b' = BU.unsafeTake 8 b
--                    in
--                     if a' == b'
--                     then GT
--                     else LT)

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
