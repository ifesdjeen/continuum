{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Serialization.Base where

import           Continuum.Types
import           Continuum.Serialization.DbValue ( encodeValue, decodeValue )
import           Continuum.Serialization.Primitive

import qualified Data.ByteString      as B
import qualified Data.Map             as Map

import           Data.Serialize       ( runPut, putWord8, putByteString, encode, decode )
import           Data.List            ( elemIndex )
import           Data.Maybe           ( isJust, fromJust, catMaybes )
import           Control.Monad.Except ( forM_, throwError )

-- import Debug.Trace

data Success = Success

-- validate :: DbSchema -> DbRecord -> Either String Success
-- validate = error "Not Implemented"

-- |
-- | ENCODING
-- |

encodeSchema :: DbSchema -> B.ByteString
encodeSchema = encode

decodeSchema :: Decoder (DbName, DbSchema)
decodeSchema (dbName, encodedSchema) =
  case (decode encodedSchema) of
    (Left err)     -> throwError $ SchemaDecodingError err
    (Right schema) -> return $! (dbName, schema)

-- |
-- | DECODING
-- |




decodeChunkKey :: Decoder Integer
decodeChunkKey (x, _) = unpackWord64 (B.take 8 x)
{-# INLINE decodeChunkKey #-}
