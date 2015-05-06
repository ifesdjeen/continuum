{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Serialization.Schema where

import           Continuum.Types
import           Continuum.Serialization.Primitive

import qualified Data.ByteString      as B
import qualified Data.Map             as Map
import           Data.ByteString      ( ByteString )
import           Data.Serialize       ( encode, decode )
import           Control.Monad.Except ( throwError )


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

-- | Creates a DbSchema out of Schema Definition (name/type pairs)
--
makeSchema :: [(ByteString, DbType)] -> DbSchema
makeSchema stringTypeList =
  DbSchema { fieldMappings  = fMappings
           , fields         = fields'
           , schemaMappings = Map.fromList stringTypeList
           , indexMappings  = iMappings
           , schemaTypes    = schemaTypes'}
  where fields'      = fmap fst stringTypeList
        schemaTypes' = fmap snd stringTypeList
        fMappings    = Map.fromList $ zip fields' iterateFrom0
        iMappings    = Map.fromList $ zip iterateFrom0 fields'
        iterateFrom0 = (iterate (1+) 0)
