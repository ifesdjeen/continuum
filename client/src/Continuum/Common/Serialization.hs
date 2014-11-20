{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Common.Serialization where

import           Foreign
import           Continuum.Common.Types

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
encodeRecord schema (DbRecord timestamp vals) sid = (encodeKey, encodeValue)
  where encodeKey = B.concat [(packWord64 timestamp), (packWord64 sid)]
        encodedParts = fmap fastEncodeValue $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths = B.length <$> encodedParts
        encodeValue = runPut $ do
          -- Change to B.Pack
          forM_ lengths (putWord8 . fromIntegral)
          forM_ encodedParts putByteString
          -- encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

encodeBeginTimestamp :: Integer -> B.ByteString
encodeBeginTimestamp timestamp =
  B.concat [(packWord64 timestamp), (packWord64 0)]

encodeEndTimestamp :: Integer -> B.ByteString
encodeEndTimestamp timestamp =
  B.concat [(packWord64 timestamp), (packWord64 999999)]

encodeSchema :: DbSchema -> B.ByteString
encodeSchema = encode

decodeSchema :: (DbName, B.ByteString)
                -> DbErrorMonad DbResult
decodeSchema (dbName, encodedSchema) =
  case (decode encodedSchema) of
    (Left err)     -> throwError $ SchemaDecodingError err
    (Right schema) -> return $ DbSchemaResult (dbName, schema)

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
             -> Decoder

decodeRecord (Field field) schema !(k, bs) = do
  decodedK      <- decodeKey k
  decodedVal    <- if isJust idx
                   then decodeFieldByIndex schema indices (fromJust idx) bs
                   else throwError FieldNotFoundError
  return $! FieldRes (decodedK, decodedVal)
  where idx     = elemIndex field (fields schema)
        indices = decodeIndexes schema bs

decodeRecord Record schema !(k, bs) = do
  timestamp      <- decodeKey k
  decodedValue   <- decodeValues schema bs
  return $! RecordRes $ DbRecord timestamp (Map.fromList $ zip (fields schema) decodedValue)

decodeRecord (Fields flds) schema (k, bs) = do
  decodedK      <- decodeKey k
  decodedVal    <- if isJust idxs
                   then mapM (\idx -> decodeFieldByIndex schema (decodeIndexes schema bs) idx bs) (fromJust idxs)
                   else throwError FieldNotFoundError
  return $! FieldsRes (decodedK, decodedVal)
  where
    {-# INLINE idxs #-}
    idxs         = mapM (`elemIndex` (fields schema)) flds
{-# INLINE decodeRecord #-}

-- |
-- | DECODING Utility Functions
-- |

decodeKey :: B.ByteString -> DbErrorMonad Integer
decodeKey x = unpackWord64 (B.take 8 x)
{-# INLINE decodeKey #-}

decodeIndexes :: DbSchema -> B.ByteString -> [Int]
decodeIndexes schema bs =
  map fromIntegral $ B.unpack $ B.take (length (fields schema)) bs
{-# INLINE decodeIndexes #-}

slide :: [a] -> [(a, a)]
slide (f:s:xs) = (f,s) : slide (s:xs)
slide _ = []

decodeValues :: DbSchema -> B.ByteString -> DbErrorMonad [DbValue]
decodeValues schema bs = mapM (\(t, s) -> fastDecodeValue t s) (zip (schemaTypes schema) bytestrings)
  where
    indices                  = decodeIndexes schema bs
    bytestrings              = snd (foldl step (B.drop (length indices) bs, []) indices)
    step (remaining', acc) n = (B.drop n remaining', acc ++ [B.take n remaining'])

-- | Decodes field by index
decodeFieldByIndex :: DbSchema
                      -> [Int]
                      -> Int
                      -> B.ByteString
                      -> DbErrorMonad DbValue
decodeFieldByIndex schema indices idx bs = fastDecodeValue ((schemaTypes schema) !! idx) bytestring
  where
    {-# INLINE bytestring #-}
    bytestring = B.take (indices !! idx) $ B.drop startFrom bs
    {-# INLINE startFrom #-}
    startFrom = (length indices) + (sum $ take idx indices)
{-# INLINE decodeFieldByIndex #-}


-- |
-- | "FAST" CUSTOM SERIALIZATION
-- |

fastEncodeValue :: DbValue -> B.ByteString
fastEncodeValue (DbInt    value) = packWord64 value
fastEncodeValue (DbString value) = value

fastDecodeValue :: DbType -> B.ByteString -> DbErrorMonad DbValue
fastDecodeValue DbtInt bs    = DbInt <$> unpackWord64 bs
fastDecodeValue DbtString bs = return $ DbString bs

packWord64 :: Integer -> B.ByteString
packWord64 i =
  let w = (fromIntegral i :: Word64)
  in B.pack [ (fromIntegral (w `shiftR` 56) :: Word8)
             , (fromIntegral (w `shiftR` 48) :: Word8)
             , (fromIntegral (w `shiftR` 40) :: Word8)
             , (fromIntegral (w `shiftR` 32) :: Word8)
             , (fromIntegral (w `shiftR` 24) :: Word8)
             , (fromIntegral (w `shiftR` 16) :: Word8)
             , (fromIntegral (w `shiftR`  8) :: Word8)
             , (fromIntegral (w)             :: Word8)]
{-# INLINE packWord64 #-}

unpackWord64 :: B.ByteString -> Either DbError Integer
unpackWord64 s = return $
    (fromIntegral (s `B.index` 0) `shiftL` 56) .|.
    (fromIntegral (s `B.index` 1) `shiftL` 48) .|.
    (fromIntegral (s `B.index` 2) `shiftL` 40) .|.
    (fromIntegral (s `B.index` 3) `shiftL` 32) .|.
    (fromIntegral (s `B.index` 4) `shiftL` 24) .|.
    (fromIntegral (s `B.index` 5) `shiftL` 16) .|.
    (fromIntegral (s `B.index` 6) `shiftL`  8) .|.
    (fromIntegral (s `B.index` 7) )
{-# INLINE unpackWord64 #-}

-- Group Queries

byField :: B.ByteString -> DbRecord -> DbValue
byField f (DbRecord _ m) = fromJust $ Map.lookup f m

byFieldMaybe :: B.ByteString -> DbRecord -> Maybe DbValue
byFieldMaybe f (DbRecord _ m) = Map.lookup f m

byTime :: Integer -> DbRecord -> Integer
byTime interval (DbRecord t _) = interval * (t `quot` interval)


unpackString :: DbValue -> B.ByteString
unpackString (DbString i) = i
unpackString _ = error "Can't unpack Int"

unpackInt :: DbValue -> Integer
unpackInt (DbInt i) = i
unpackInt _ = error "Can't unpack Int"

unpackFloat :: DbValue -> Float
unpackFloat (DbFloat i) = i
unpackFloat _ = error "Can't unpack Float"

unpackDouble :: DbValue -> Double
unpackDouble (DbDouble i) = i
unpackDouble _ = error "Can't unpack Double"
