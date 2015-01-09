{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Storage.C where

import qualified Data.ByteString.Unsafe    as BU

import           Control.Exception         ( bracket )
import           Database.LevelDB.Base     ( ReadOptions(..) )
import           Database.LevelDB.C        ( CompareFun, LevelDBPtr, ReadOptionsPtr,
                                                  LevelDBPtr, mkCmp )
import           Database.LevelDB.Internal ( DB(..), mkCompareFun, mkCReadOpts )
import           Control.Applicative       ( (<*>), (<$>) )
import           Data.ByteString           ( ByteString, packCStringLen )

import           Continuum.Common.Primitive
import           Continuum.Common.Types

import           Foreign
import           Foreign.C.Types
import           Foreign.C.String
import           Data.IORef                ( IORef(..), newIORef, modifyIORef', readIORef )

import Debug.Trace

#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)
#include "continuum.h"

-- |
-- | FFI Decoding
-- |

decodeString :: CString -> CSize -> IO ByteString
decodeString ptr size = packCStringLen (ptr, fromIntegral size)
{-# INLINE decodeString #-}

scan :: (Show a) => DB
        -> ReadOptions
        -> ScanRange
        -> Decoder a
        -> IO (DbErrorMonad [a])

scan (DB dbPtr _) ro scanRange decoder = do
  cReadOpts <- mkCReadOpts ro
  ref       <- newIORef []
  appendFn  <- makeCAppendFun $ makeAppendFun ref decoder
  ()        <- doScan dbPtr cReadOpts scanRange appendFn
  result    <- readIORef ref
  return $! (\i -> dropRangeParts scanRange <$> i) <$> sequence $ result

doScan :: LevelDBPtr
              -> ReadOptionsPtr
              -> ScanRange
              -> AppendFunPtr
              -> IO ()
doScan db ro EntireKeyspace appendFn = scan_entire_keyspace db ro appendFn

doScan db ro (OpenEnd rangeStart) appendFn =
  let rangeStartBytes = packWord64 rangeStart
  in
   BU.unsafeUseAsCStringLen rangeStartBytes $ \(start_at_ptr, start_at_len) ->
   scan_open_end db ro start_at_ptr (fromIntegral start_at_len) appendFn

doScan db ro (KeyRange rangeStart rangeEnd) appendFn =
  let rangeStartBytes = packWord64 rangeStart
      rangeEndBytes   = packWord64 rangeEnd
  in
   BU.unsafeUseAsCStringLen rangeStartBytes $ \(start_at_ptr, start_at_len) ->
   BU.unsafeUseAsCStringLen rangeEndBytes   $ \(end_at_ptr, end_at_len) -> do
     comparator <- mkCmp $ bitwise_compare
     scan_range db ro start_at_ptr (fromIntegral start_at_len) end_at_ptr (fromIntegral end_at_len) comparator appendFn

doScan db ro (OpenEndButFirst rangeStart)         appendFn = doScan db ro (OpenEnd rangeStart) appendFn
doScan db ro (ButFirst rangeStart rangeEnd)       appendFn = doScan db ro (KeyRange rangeStart rangeEnd) appendFn
doScan db ro (ButLast rangeStart rangeEnd)        appendFn = doScan db ro (KeyRange rangeStart rangeEnd) appendFn
doScan db ro (ExclusiveRange rangeStart rangeEnd) appendFn = doScan db ro (KeyRange rangeStart rangeEnd) appendFn

dropRangeParts (OpenEndButFirst _) range  = drop 1 $ range
dropRangeParts (ButFirst _ _) range       = drop 1 $ range
dropRangeParts (ButLast _ _) range        = take ((length range) - 1) range
dropRangeParts (ExclusiveRange _ _) range = drop 1 $ take ((length range) - 1) range
dropRangeParts _ range                    = range

-- |
-- | Foreign imports
-- |

foreign import ccall safe "continuum.h"
  scan_entire_keyspace :: LevelDBPtr
                          -> ReadOptionsPtr
                          -> FunPtr AppendFun
                          -> IO ()

foreign import ccall safe "continuum.h"
  scan_range :: LevelDBPtr
                -> ReadOptionsPtr
                -> CString
                -> CSize
                -> CString
                -> CSize
                -> FunPtr CompareFun
                -> FunPtr AppendFun
                -> IO ()

foreign import ccall safe "continuum.h"
  scan_open_end :: LevelDBPtr
                   -> ReadOptionsPtr
                   -> CString
                   -> CSize
                   -> FunPtr AppendFun
                   -> IO ()

foreign import ccall safe "static continuum.h"
  bitwise_compare :: CompareFun

-- |
-- | Comparator functions
-- |

bitwise_compare_hs :: CompareFun
bitwise_compare_hs =
  mkCompareFun $ (\a b ->
                   let a' = BU.unsafeTake 8 a
                       b' = BU.unsafeTake 8 b
                   in
                    compare a' b')

prefix_eq_comparator :: CompareFun
prefix_eq_comparator =
  mkCompareFun $ (\a b ->
                   let a' = BU.unsafeTake 8 a
                       b' = BU.unsafeTake 8 b
                   in
                    if a' == b'
                    then GT
                    else LT)

type AppendFun = CString -> CSize -> CString -> CSize -> IO ()

makeAppendFun :: (Show a) => IORef [DbErrorMonad a] -> Decoder a -> AppendFun
makeAppendFun ref decoder keyPtr keyLen valPtr valLen = do
  key <- decodeString keyPtr keyLen
  val <- decodeString valPtr valLen
  modifyIORef' ref (\prev -> let next = decoder (key, val)
                             in seq next (prev ++ [next]))

type AppendFunPtr = FunPtr AppendFun

foreign import ccall safe "wrapper"
  makeCAppendFun :: AppendFun -> IO AppendFunPtr
