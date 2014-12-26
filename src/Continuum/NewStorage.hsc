{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.NewStorage where

import Control.Exception ( bracket )

import qualified Data.ByteString.Unsafe         as BU
import           Database.LevelDB.Base          ( ReadOptions(..) )
import           Database.LevelDB.C             ( CompareFun, LevelDBPtr, ReadOptionsPtr,
                                                  LevelDBPtr, mkCmp )
import           Database.LevelDB.Internal      ( DB(..), mkCompareFun, mkCReadOpts )

import           Continuum.Common.Primitive
import           Continuum.Common.Types

import           Control.Applicative ( (<*>), (<$>) )
import           Foreign
import           Foreign.C.Types
import           Foreign.C.String
import           Data.ByteString           ( ByteString, packCStringLen )

import Debug.Trace
#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)
#include "continuum.h"


data CKeyValuePair = CKeyValuePair ByteString ByteString
                   deriving(Eq, Show)

type CKeyValuePairPtr = Ptr CKeyValuePair

data CDbResults = CDbResults [CKeyValuePair] deriving(Eq, Show)

type CDbResultsPtr = Ptr CDbResults

instance Storable CKeyValuePair where
  alignment _ = #{alignment key_value_pair_t}
  sizeOf _    = #{size      key_value_pair_t}

  peek p      = do
    keyLen <- #{peek key_value_pair_t, key_len}  p
    keyPtr <- #{peek key_value_pair_t, key}      p

    valLen <- #{peek key_value_pair_t, val_len}  p
    valPtr <- #{peek key_value_pair_t, val}      p

    CKeyValuePair
      <$> decodeString keyLen keyPtr
      <*> decodeString valLen valPtr

  poke = undefined -- we never need or use that

instance Storable CDbResults where
  alignment _ = #{alignment db_results_t}
  sizeOf _    = #{size      db_results_t}

  peek p      = do
    kvpsCount  <- #{peek db_results_t, count }  p
    kvpsPtr    <- #{peek db_results_t, results} p
    kvps       <- peekArray kvpsCount kvpsPtr

    return $ CDbResults kvps

  poke = undefined -- we never need or use that

decodeString :: CSize -> CString -> IO ByteString
decodeString len res = packCStringLen (res, fromIntegral len)
{-# INLINE decodeString #-}

scan :: DB
        -> ReadOptions
        -> ScanRange
        -> Decoder a
        -> IO (DbErrorMonad [a])

scan (DB dbPtr _) ro scanRange decoder = do
  cReadOpts <- mkCReadOpts ro
  bracket (makeScanFn dbPtr cReadOpts scanRange)
          free_db_results
          (\resultsPtr -> (mapM decoder) <$> toByteStringTuple <$> peek resultsPtr)

toByteStringTuple :: CDbResults -> [(ByteString, ByteString)]
toByteStringTuple (CDbResults a) = map (\(CKeyValuePair b c) -> (b, c)) a

makeScanFn :: LevelDBPtr
             -> ReadOptionsPtr
             -> ScanRange
             -> IO CDbResultsPtr
makeScanFn db ro EntireKeyspace = scan_entire_keyspace db ro
makeScanFn db ro (OpenEnd rangeStart) =
  let rangeStartBytes = packWord64 rangeStart
  in
   BU.unsafeUseAsCStringLen rangeStartBytes $ \(start_at_ptr, start_at_len) ->
   scan_open_end db ro start_at_ptr (fromIntegral start_at_len)

makeScanFn db ro (KeyRange rangeStart rangeEnd) =
  let rangeStartBytes = packWord64 rangeStart
      rangeEndBytes   = packWord64 rangeEnd
  in
   BU.unsafeUseAsCStringLen rangeStartBytes $ \(start_at_ptr, start_at_len) ->
   BU.unsafeUseAsCStringLen rangeEndBytes   $ \(end_at_ptr, end_at_len) -> do
     comparator <- mkCmp $ bitwise_compare
     scan_range db ro start_at_ptr (fromIntegral start_at_len) end_at_ptr (fromIntegral end_at_len) comparator

makeScanFn db ro (SingleKey key) =
  let matchKey = packWord64 key
  in
   BU.unsafeUseAsCStringLen matchKey $ \(ptr, len) -> do
     comparator <- mkCmp $ prefix_eq_comparator
     scan_range db ro ptr (fromIntegral len) ptr (fromIntegral len) comparator

-- |
-- | Foreign imports
-- |

foreign import ccall safe "continuum.h"
  scan_entire_keyspace :: LevelDBPtr -> ReadOptionsPtr -> IO CDbResultsPtr

foreign import ccall safe "continuum.h"
  scan_range :: LevelDBPtr
                -> ReadOptionsPtr
                -> CString
                -> CSize
                -> CString
                -> CSize
                -> FunPtr CompareFun
                -> IO CDbResultsPtr

foreign import ccall safe "continuum.h"
  scan_open_end :: LevelDBPtr
                   -> ReadOptionsPtr
                   -> CString
                   -> CSize
                   -> IO CDbResultsPtr

foreign import ccall safe "continuum.h"
  free_db_results :: CDbResultsPtr -> IO ()

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
