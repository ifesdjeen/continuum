{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Serialization where


import           Control.Applicative ((<$>))
import           Foreign
import qualified Data.Map as Map

import           Continuum.Types
import           Data.List (elemIndex)

import           Data.Serialize as S
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS

import           Data.Maybe (isJust, fromJust, catMaybes)
import           Control.Monad.Except(forM_, throwError)

-- import Debug.Trace

data Success = Success

-- validate :: DbSchema -> DbRecord -> Either String Success
-- validate = error "Not Implemented"

-- |
-- | ENCODING
-- |

encodeRecord :: DbSchema -> DbRecord -> Integer -> (ByteString, ByteString)
encodeRecord schema (DbRecord timestamp vals) sid = (encodeKey, encodeValue)
  where encodeKey = BS.concat [(packWord64 timestamp), (packWord64 sid)]
        encodedParts = fmap fastEncodeValue $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths = BS.length <$> encodedParts
        encodeValue = runPut $ do
          -- Change to BS.Pack
          forM_ lengths (putWord8 . fromIntegral)
          forM_ encodedParts putByteString
          -- encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

encodeBeginTimestamp :: Integer -> ByteString
encodeBeginTimestamp timestamp =
  BS.concat [(packWord64 timestamp), (packWord64 0)]

encodeEndTimestamp :: Integer -> ByteString
encodeEndTimestamp timestamp =
  BS.concat [(packWord64 timestamp), (packWord64 999999)]

encodeSchema :: DbSchema -> ByteString
encodeSchema = encode

decodeSchema :: (ByteString, ByteString)
                -> DbErrorMonad (ByteString, DbSchema)
decodeSchema (dbName, encodedSchema) =
  case (decode encodedSchema) of
    (Left err)     -> throwError $ SchemaDecodingError err
    (Right schema) -> return (dbName, schema)

-- |
-- | DECODING
-- |

decodeRecord :: Decoding
             -> DbSchema
             -> (ByteString, ByteString)
             -> DbErrorMonad DbResult

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

decodeKey :: ByteString -> DbErrorMonad Integer
decodeKey x = unpackWord64 (BS.take 8 x)
{-# INLINE decodeKey #-}

decodeIndexes :: DbSchema -> ByteString -> [Int]
decodeIndexes schema bs =
  map fromIntegral $ BS.unpack $ BS.take (length (fields schema)) bs
{-# INLINE decodeIndexes #-}

slide :: [a] -> [(a, a)]
slide (f:s:xs) = (f,s) : slide (s:xs)
slide _ = []

decodeValues :: DbSchema -> ByteString -> DbErrorMonad [DbValue]
decodeValues schema bs = mapM (\(t, s) -> fastDecodeValue t s) (zip (schemaTypes schema) bytestrings)
  where
    indices                  = decodeIndexes schema bs
    bytestrings              = snd (foldl step (BS.drop (length indices) bs, []) indices)
    step (remaining', acc) n = (BS.drop n remaining', acc ++ [BS.take n remaining'])

-- | Decodes field by index
decodeFieldByIndex :: DbSchema
                      -> [Int]
                      -> Int
                      -> ByteString
                      -> DbErrorMonad DbValue
decodeFieldByIndex schema indices idx bs = fastDecodeValue ((schemaTypes schema) !! idx) bytestring
  where
    {-# INLINE bytestring #-}
    bytestring = BS.take (indices !! idx) $ BS.drop startFrom bs
    {-# INLINE startFrom #-}
    startFrom = (length indices) + (sum $ take idx indices)
{-# INLINE decodeFieldByIndex #-}


-- |
-- | "FAST" CUSTOM SERIALIZATION
-- |

fastEncodeValue :: DbValue -> ByteString
fastEncodeValue (DbInt    value) = packWord64 value
fastEncodeValue (DbString value) = value

fastDecodeValue :: DbType -> ByteString -> DbErrorMonad DbValue
fastDecodeValue DbtInt bs    = DbInt <$> unpackWord64 bs
fastDecodeValue DbtString bs = return $ DbString bs

packWord64 :: Integer -> ByteString
packWord64 i =
  let w = (fromIntegral i :: Word64)
  in BS.pack [ (fromIntegral (w `shiftR` 56) :: Word8)
             , (fromIntegral (w `shiftR` 48) :: Word8)
             , (fromIntegral (w `shiftR` 40) :: Word8)
             , (fromIntegral (w `shiftR` 32) :: Word8)
             , (fromIntegral (w `shiftR` 24) :: Word8)
             , (fromIntegral (w `shiftR` 16) :: Word8)
             , (fromIntegral (w `shiftR`  8) :: Word8)
             , (fromIntegral (w)             :: Word8)]
{-# INLINE packWord64 #-}

unpackWord64 :: ByteString -> Either DbError Integer
unpackWord64 s = return $
    (fromIntegral (s `BS.index` 0) `shiftL` 56) .|.
    (fromIntegral (s `BS.index` 1) `shiftL` 48) .|.
    (fromIntegral (s `BS.index` 2) `shiftL` 40) .|.
    (fromIntegral (s `BS.index` 3) `shiftL` 32) .|.
    (fromIntegral (s `BS.index` 4) `shiftL` 24) .|.
    (fromIntegral (s `BS.index` 5) `shiftL` 16) .|.
    (fromIntegral (s `BS.index` 6) `shiftL`  8) .|.
    (fromIntegral (s `BS.index` 7) )
{-# INLINE unpackWord64 #-}

-- Group Queries

byField :: ByteString -> DbRecord -> DbValue
byField f (DbRecord _ m) = fromJust $ Map.lookup f m
byField _ _ = DbInt 1 -- WTF

byFieldMaybe :: ByteString -> DbRecord -> Maybe DbValue
byFieldMaybe f (DbRecord _ m) = Map.lookup f m
byFieldMaybe _ _              = Just $ DbInt 1 -- WTF

byTime :: Integer -> DbRecord -> Integer
byTime interval (DbRecord t _) = interval * (t `quot` interval)


unpackString :: DbValue -> ByteString
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
