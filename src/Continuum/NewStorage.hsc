{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.NewStorage where

import Control.Exception ( bracket )

import qualified Database.LevelDB.Base          as LDB
import qualified Database.LevelDB.C             as CLDB
import qualified Database.LevelDB.Internal      as CLDBI
import qualified Data.Map.Strict                as Map
import           Continuum.Common.Serialization

import Continuum.Common.Types
import           Continuum.Context

import Control.Applicative ( (<*>), (<$>) )
import Foreign
import Foreign.C.Types
import Foreign.C.String
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
    keyPtr <- #{peek key_value_pair_t, key} p

    valLen <- #{peek key_value_pair_t, val_len}  p
    valPtr <- #{peek key_value_pair_t, val}  p

    CKeyValuePair <$> (decodeString keyLen keyPtr) <*> (decodeString valLen valPtr)

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


scanKeyspace :: DbContext
                -> DbName
                -> IO (DbErrorMonad [DbResult])

scanKeyspace context dbName = do
  let maybeDb = Map.lookup dbName (ctxDbs context)
      ro      = fst $ ctxRwOptions context

  cReadOpts <- CLDBI.mkCReadOpts ro

  case maybeDb of
    (Just (schema, CLDBI.DB dbPtr _)) -> do
      let decoder = decodeRecord Record schema

      bracket (scan_entire_keyspace dbPtr cReadOpts)
              free_db_results
              (\resultsPtr -> (mapM decoder) <$> toByteStringTuple <$> peek resultsPtr)

    Nothing             -> return $ Left NoSuchDatabaseError


toByteStringTuple :: CDbResults -> [(ByteString, ByteString)]
toByteStringTuple (CDbResults a) = map (\(CKeyValuePair b c) -> (b, c)) a


foreign import ccall safe "continuum.h scan_entire_keyspace"
  scan_entire_keyspace :: CLDB.LevelDBPtr -> CLDB.ReadOptionsPtr -> IO CDbResultsPtr

foreign import ccall safe "continuum.h free_db_results"
  free_db_results :: CDbResultsPtr -> IO ()
