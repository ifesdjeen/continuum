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

import           Continuum.Serialization.Primitive
import           Continuum.Types

import           Foreign
import           Foreign.C.Types
import           Foreign.C.String
import           Data.IORef                ( IORef(..), newIORef, modifyIORef', readIORef )

import           Control.Monad ( when )

import qualified Control.Foldl                  as L
import qualified Data.Map.Strict                as Map
import           Continuum.Serialization.Base
import           Continuum.Context
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
  scanRes  <- doScan dbPtr cReadOpts scanRange appendFn
  result   <- readIORef ref
  return $! done <$> result
  -- return $! (\i -> dropRangeParts scanRange <$> i) <$> done <$> result


-- scan :: DB
--         -> ReadOptions
--         -> ScanRange
--         -> Decoder a
--         -> IO (DbErrorMonad [a])

-- scan (DB dbPtr _) ro scanRange decoder = do
--   cReadOpts <- mkCReadOpts ro
--   ref       <- newIORef []
--   appendFn  <- makeCStepFun $ makeStepFun ref decoder
--   ()        <- doScan dbPtr cReadOpts scanRange appendFn
--   result    <- readIORef ref
--   return $! (\i -> dropRangeParts scanRange <$> i) <$> sequence $ result

doScan :: LevelDBPtr
          -> ReadOptionsPtr
          -> ScanRange
          -> StepFunPtr
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
                          -> FunPtr StepFun
                          -> IO ()

foreign import ccall safe "continuum.h"
  scan_range :: LevelDBPtr
                -> ReadOptionsPtr
                -> CString
                -> CSize
                -> CString
                -> CSize
                -> FunPtr CompareFun
                -> FunPtr StepFun
                -> IO ()

foreign import ccall safe "continuum.h"
  scan_open_end :: LevelDBPtr
                   -> ReadOptionsPtr
                   -> CString
                   -> CSize
                   -> FunPtr StepFun
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

type StepFun = CString -> CSize -> CString -> CSize -> IO ()

makeStepFun :: IORef (DbErrorMonad x) -> Decoder a -> (x -> a -> x) -> StepFun
makeStepFun ref decoder step keyPtr keyLen valPtr valLen = do
  key <- decodeString keyPtr keyLen
  val <- decodeString valPtr valLen
  modifyIORef' ref (\prev -> step <$> prev <*> (decoder (key, val))) -- Do we need forcing seq here?
{-# INLINE makeStepFun #-}

type StepFunPtr = FunPtr StepFun

foreign import ccall safe "wrapper"
  makeCStepFun :: StepFun -> IO StepFunPtr
