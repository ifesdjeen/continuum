{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.NewStorage where

import Control.Exception ( bracket )

import qualified Data.ByteString.Unsafe         as BU
import qualified Database.LevelDB.Base          as LDB
import qualified Database.LevelDB.C             as CLDB
import qualified Database.LevelDB.Internal      as CLDBI
import qualified Data.Map.Strict                as Map

import           Continuum.Common.Primitive
import           Continuum.Common.Serialization
import           Continuum.Common.Types
import           Continuum.Context

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

scan :: LDB.DB
        -> LDB.ReadOptions
        -> ScanRange
        -> Decoder a
        -> IO (DbErrorMonad [a])

scan (CLDBI.DB dbPtr _) ro scanRange decoder = do
  cReadOpts <- CLDBI.mkCReadOpts ro

  bracket (makeScanFn dbPtr cReadOpts scanRange)
          free_db_results
          (\resultsPtr -> (mapM decoder) <$> toByteStringTuple <$> peek resultsPtr)

toByteStringTuple :: CDbResults -> [(ByteString, ByteString)]
toByteStringTuple (CDbResults a) = map (\(CKeyValuePair b c) -> (b, c)) a

foreign import ccall safe "continuum.h scan_entire_keyspace"
  scan_entire_keyspace :: CLDB.LevelDBPtr -> CLDB.ReadOptionsPtr -> IO CDbResultsPtr

foreign import ccall safe "continuum.h scan_range"
  scan_range :: CLDB.LevelDBPtr
                -> CLDB.ReadOptionsPtr
                -> CString
                -> CSize
                -> CString
                -> CSize
                -> FunPtr CLDB.CompareFun
                -> IO CDbResultsPtr

foreign import ccall safe "continuum.h free_db_results"
  free_db_results :: CDbResultsPtr -> IO ()

makeScanFn :: CLDB.LevelDBPtr
             -> CLDB.ReadOptionsPtr
             -> ScanRange
             -> IO CDbResultsPtr
makeScanFn db ro EntireKeyspace = scan_entire_keyspace db ro
makeScanFn db ro (KeyRange rangeStart rangeEnd) =
  let rangeStartBytes = packWord64 rangeStart
      rangeEndBytes   = packWord64 rangeEnd
  in
   BU.unsafeUseAsCStringLen rangeStartBytes $ \(start_at_ptr, start_at_len) ->
   BU.unsafeUseAsCStringLen rangeEndBytes   $ \(end_at_ptr, end_at_len) -> do
     comparator <- CLDB.mkCmp . CLDBI.mkCompareFun $ (\a b ->
                                                       let a' = BU.unsafeTake 8 a
                                                           b' = BU.unsafeTake 8 b
                                                       in
                                                        compare (trace (show $ unpackWord64 a') a') (trace (show $ unpackWord64 b') b'))
     scan_range db ro start_at_ptr (fromIntegral start_at_len) end_at_ptr (fromIntegral end_at_len) comparator
