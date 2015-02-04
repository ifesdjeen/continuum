{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Serialization.Base where

import           Continuum.Types
import           Continuum.Serialization.Primitive

import           Data.Serialize       as S
import qualified Data.ByteString      as B
import qualified Data.Map             as Map

import           Control.Applicative  ( (<$>) )
import           Data.List            ( elemIndex )
import           Data.Maybe           ( isJust, fromJust, catMaybes )
import           Control.Monad.Except ( forM_, throwError )
import           Control.Monad        ( join )

-- import Debug.Trace

data Success = Success

-- validate :: DbSchema -> DbRecord -> Either String Success
-- validate = error "Not Implemented"

-- |
-- | ENCODING
-- |

encodeRecord :: DbSchema -> DbRecord -> Integer -> (B.ByteString, B.ByteString)
encodeRecord schema (DbRecord timestamp vals) sid = (encodedKey, encodedValue)
  where encodedKey = B.concat [(packWord64 timestamp), (packWord64 sid)]
        encodedParts = fmap encodeValue $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths = B.length <$> encodedParts
        encodedValue = runPut $ do
          -- Change to B.Pack
          forM_ lengths (putWord8 . fromIntegral)
          forM_ encodedParts putByteString
          -- encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

encodeSchema :: DbSchema -> B.ByteString
encodeSchema = encode

decodeSchema :: Decoder (DbName, DbSchema)
decodeSchema (dbName, encodedSchema) =
  case (decode encodedSchema) of
    (Left err)     -> throwError $ SchemaDecodingError err
    (Right schema) -> return $! (dbName, schema)

decodeQuery :: B.ByteString
               -> DbErrorMonad SelectQuery
decodeQuery encodedQuery =
  case (decode encodedQuery) of
    (Left err)     -> throwError $ SchemaDecodingError err
    (Right query)  -> return query

decodeDbResult :: B.ByteString
               -> DbErrorMonad DbResult
decodeDbResult encodedDbResult =
  case (decode encodedDbResult) of
    (Left err)     -> throwError $ SchemaDecodingError err
    (Right query)  -> join $ return query


-- |
-- | DECODING
-- |

decodeRecord :: Decoding
             -> DbSchema
             -> Decoder DbRecord

decodeRecord (Field field) schema !(k, bs) = do
  timestamp     <- decodeKey k
  decodedVal    <- if isJust idx
                   then decodeFieldByIndex schema indices (fromJust idx) bs
                   else throwError FieldNotFoundError
  return $! DbRecord timestamp (Map.fromList $ [(field, decodedVal)])
  where idx     = elemIndex field (fields schema)
        indices = decodeIndexes schema bs

decodeRecord Record schema !(k, bs) = do
  timestamp      <- decodeKey k
  decodedVal     <- decodeValues schema bs
  return $! DbRecord timestamp (Map.fromList $ zip (fields schema) decodedVal)

decodeRecord Key _ !(k, _) = do
  timestamp      <- decodeKey k
  return $! DbRecord timestamp (Map.fromList [])

decodeRecord (Fields flds) schema (k, bs) = do
  timestamp     <- decodeKey k
  decodedVals   <- if isJust idxs
                   then mapM (\idx -> decodeFieldByIndex schema (decodeIndexes schema bs) idx bs) (fromJust idxs)
                   else throwError FieldNotFoundError
  return $! DbRecord timestamp (Map.fromList $ zip flds decodedVals)
  where
    {-# INLINE idxs #-}
    idxs         = mapM (`elemIndex` (fields schema)) flds
{-# INLINE decodeRecord #-}

-- |
-- | DECODING Utility Functions
-- |

decodeKey :: B.ByteString -> DbErrorMonad Integer
decodeKey = unpackWord64
{-# INLINE decodeKey #-}

decodeChunkKey :: Decoder Integer
decodeChunkKey (x, _) = unpackWord64 (B.take 8 x)
{-# INLINE decodeChunkKey #-}

decodeIndexes :: DbSchema -> B.ByteString -> [Int]
decodeIndexes schema bs =
  map fromIntegral $ B.unpack $ B.take (length (fields schema)) bs
{-# INLINE decodeIndexes #-}

decodeValues :: DbSchema -> B.ByteString -> DbErrorMonad [DbValue]
decodeValues schema bs = mapM (\(t, s) -> decodeValue t s) (zip (schemaTypes schema) bytestrings)
  where
    indices                  = decodeIndexes schema bs
    bytestrings              = snd (foldl step (B.drop (length indices) bs, []) indices)
    step (remaining', acc) n = (B.drop n remaining', acc ++ [B.take n remaining'])
{-# INLINE decodeValues #-}

-- | Decodes field by index
decodeFieldByIndex :: DbSchema
                      -> [Int]
                      -> Int
                      -> B.ByteString
                      -> DbErrorMonad DbValue
decodeFieldByIndex schema indices idx bs = decodeValue ((schemaTypes schema) !! idx) bytestring
  where
    {-# INLINE bytestring #-}
    bytestring = B.take (indices !! idx) $ B.drop startFrom bs
    {-# INLINE startFrom #-}
    startFrom = (length indices) + (sum $ take idx indices)
{-# INLINE decodeFieldByIndex #-}


-- |
-- | "FAST" CUSTOM SERIALIZATION
-- |

encodeValue :: DbValue -> B.ByteString
encodeValue (DbLong   value) = packWord64 value
encodeValue (DbInt    value) = packWord32 value
encodeValue (DbShort  value) = packWord16 value
encodeValue (DbByte   value) = packWord8  value
encodeValue (DbFloat  value) = packFloat value
encodeValue (DbDouble value) = packDouble value
encodeValue (DbString value) = value

decodeValue :: DbType -> B.ByteString -> DbErrorMonad DbValue
decodeValue DbtLong    bs  = DbLong   <$> (unpackWord64 bs)
decodeValue DbtInt     bs  = DbInt    <$> (unpackWord32 bs)
decodeValue DbtShort   bs  = DbShort  <$> (unpackWord16 bs)
decodeValue DbtByte    bs  = DbByte   <$> (unpackWord8 bs)
decodeValue DbtFloat   bs  = DbFloat  <$> (unpackFloat bs)
decodeValue DbtDouble  bs  = DbDouble <$> (unpackDouble bs)
decodeValue DbtString  bs  = return $ DbString bs
