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
--import Debug.Trace

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
doScan db ro EntireKeyspace appendFn = scan_entire_keyspace db ro appendFn

doScan db ro (OpenEnd rangeStart) appendFn =
  BU.unsafeUseAsCStringLen rangeStart $ \(start_at_ptr, start_at_len) ->
  scan_open_end db ro start_at_ptr (fromIntegral start_at_len) appendFn

doScan db ro (KeyRange rangeStart rangeEnd) appendFn =
  BU.unsafeUseAsCStringLen rangeStart $ \(start_at_ptr, start_at_len) ->
  BU.unsafeUseAsCStringLen rangeEnd   $ \(end_at_ptr, end_at_len)     -> do
    let start_at_len' = fromIntegral start_at_len
        end_at_len'   = fromIntegral end_at_len
    comparator <- mkCmp $ (\p1 l1 -> bitwiseCompare p1 l1 end_at_ptr end_at_len')
    scan_range db ro start_at_ptr start_at_len' comparator appendFn

doScan db ro (ButFirst rangeStart rangeEnd) appendFn =
  BU.unsafeUseAsCStringLen rangeStart $ \(start_at_ptr, start_at_len) ->
  BU.unsafeUseAsCStringLen rangeEnd   $ \(end_at_ptr, end_at_len)     -> do
    let start_at_len' = fromIntegral start_at_len
        end_at_len'   = fromIntegral end_at_len
    comparator <- mkCmp $ (\p1 l1 -> bitwiseCompare p1 l1 end_at_ptr end_at_len')
    scan_range_butfirst db ro start_at_ptr start_at_len' comparator appendFn

doScan db ro (OpenEndButFirst rangeStart) appendFn =
  BU.unsafeUseAsCStringLen rangeStart $ \(start_at_ptr, start_at_len) ->
  scan_open_end_butfirst db ro start_at_ptr (fromIntegral start_at_len) appendFn

-- doScan db ro (ButLast rangeStart rangeEnd) appendFn = doScan db ro (KeyRange rangeStart rangeEnd) appendFn

-- |
-- | Foreign imports
-- |

foreign import ccall safe "continuum.h"
  scan_entire_keyspace :: LevelDBPtr
                          -> ReadOptionsPtr
                          -> FunPtr StepFun
                          -> IO ()

foreign import ccall safe "continuum.h"
  scan_range :: LevelDBPtr
                -> ReadOptionsPtr
                -> CString
                -> CSize
                -> FunPtr CurriedCompareFun
                -> FunPtr StepFun
                -> IO ()

foreign import ccall safe "continuum.h"
  scan_range_butfirst :: LevelDBPtr
                      -> ReadOptionsPtr
                      -> CString
                      -> CSize
                      -> FunPtr CurriedCompareFun
                      -> FunPtr StepFun
                      -> IO ()

foreign import ccall safe "continuum.h"
  scan_open_end :: LevelDBPtr
                   -> ReadOptionsPtr
                   -> CString
                   -> CSize
                   -> FunPtr StepFun
                   -> IO ()

foreign import ccall safe "continuum.h"
  scan_open_end_butfirst :: LevelDBPtr
                         -> ReadOptionsPtr
                         -> CString
                         -> CSize
                         -> FunPtr StepFun
                         -> IO ()

foreign import ccall safe "continuum.h bitwise_compare"
  ___bitwise_compare :: CompareFun

bitwiseCompare :: CString -> CSize -> CString -> CSize -> IO CInt
bitwiseCompare p1 s1 p2 s2 = do
  p <- peek $ nullPtr
  ___bitwise_compare p p1 s1 p2 s2

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
