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

import Debug.Trace

data Success = Success

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

-- TODO: change String to ByteString
-- overloadedstrings

makeRecord :: Integer -> [(ByteString, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

validate :: DbSchema -> DbRecord -> Either String Success
validate = error "Not Implemented"

fastEncodeValue :: DbValue -> ByteString
fastEncodeValue (DbInt    value) = packWord64 value
fastEncodeValue (DbString value) = value

fastDecodeValue :: DbType -> ByteString -> Either DbError DbValue
fastDecodeValue DbtInt bs    = return $ DbInt $ unpackWord64 bs
fastDecodeValue DbtString bs = return $ DbString bs

-- | Decode a single key (mostly a wrapper over @decode, giving a more concrete
-- Error type)
decodeKey :: ByteString -> Either DbError Integer
decodeKey x = return $ unpackWord64 (BS.take 8 x)

{-# INLINE decodeKey #-}
decodeRecord :: DbSchema ->  (ByteString, ByteString) -> Either DbError DbRecord
decodeRecord schema (k, v) = do
  timestamp      <- decodeKey k
  decodedValue   <- decodeValues schema v
  return $ DbRecord timestamp (Map.fromList $ zip (fields schema) decodedValue)

-- unwrapRecord :: Either String DbRecord -> DbRecord
-- unwrapRecord (Right x) = x

encodeBeginTimestamp :: Integer -> ByteString
encodeBeginTimestamp timestamp = BS.concat [(packWord64 timestamp), (packWord64 0)]

-- isSameTimestamp :: Integer -> DbRecord -> a -> Bool
-- isSameTimestamp begin (DbRecord current _) _ = begin == current
-- isSameTimestamp begin (DbPlaceholder current) _ = begin == current
-- isSameTimestamp begin _ _ = False

class EndCriteria a where
  matchEnd :: (Integer -> Integer -> Bool)
              -> Integer
              -> a
              -> Bool

instance EndCriteria (DbRecord) where
  matchEnd op rangeEnd (DbRecord current _) = current `op` rangeEnd
  matchEnd op rangeEnd (DbPlaceholder current) = current `op` rangeEnd

instance EndCriteria (Integer, DbValue) where
  matchEnd op rangeEnd (current, _) = current `op` rangeEnd

instance EndCriteria (Integer, [DbValue]) where
  matchEnd op rangeEnd (current, _) = current `op` rangeEnd

matchTs :: (EndCriteria i) =>
           (Integer -> Integer -> Bool)
           -> Integer
           -> i
           -> acc
           -> Bool

matchTs op rangeEnd item _ = matchEnd op rangeEnd item

byField :: ByteString -> DbRecord -> DbValue
byField f (DbRecord _ m) = fromJust $ Map.lookup f m
byField _ _ = DbInt 1 -- WTF

byFieldMaybe :: ByteString -> DbRecord -> Maybe DbValue
byFieldMaybe f (DbRecord _ m) = Map.lookup f m
byFieldMaybe _ _ = Just $ DbInt 1 -- WTF

byTime :: Integer -> DbRecord -> Integer
byTime interval (DbRecord t _) = interval * (t `quot` interval)

-- Add multi-groups for grouping via multiple fields / preds

encodeRecord :: DbSchema -> DbRecord -> Integer -> (ByteString, ByteString)
encodeRecord schema (DbRecord timestamp vals) sid = (encodeKey, encodeValue)
  where encodeKey = BS.concat [(packWord64 timestamp), (packWord64 sid)]
        -- encode (timestamp, sid)
        -- encodedVals = fmap encode $ catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)
        encodedParts = fmap fastEncodeValue $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths = BS.length <$> encodedParts
        encodeValue = runPut $ do
          -- Change to BS.Pack
          forM_ lengths (putWord8 . fromIntegral)
          forM_ encodedParts putByteString
          -- encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

decodeIndexes :: DbSchema -> ByteString -> [Int]
decodeIndexes schema bs = map fromIntegral (BS.unpack (BS.take (length (fields schema)) bs))
{-# INLINE decodeIndexes #-}

slide :: [Int] -> [(Int, Int)]
slide (f:s:xs) = (f,s) : slide (s:xs)
slide _ = []

decodeValues :: DbSchema -> ByteString -> Either DbError [DbValue]
decodeValues schema bs = mapM (\(t, s) -> fastDecodeValue t s) (zip (schemaTypes schema) bytestrings)
  where
    indices                  = decodeIndexes schema bs
    bytestrings              = snd (foldl step (BS.drop (length indices) bs, []) indices)
    step (remaining', acc) n = (BS.drop n remaining', acc ++ [BS.take n remaining'])
                            -- where decodeAll = runGet $ do idx <- forM (fields schema) (\_ -> getWord8)
                            --                               forM idx (\c -> getBytes (fromIntegral c))

-- | Decodes field by index
decodeFieldByIndex :: DbSchema
                      -> [Int]
                      -> Int
                      -> ByteString
                      -> Either DbError DbValue
decodeFieldByIndex schema indices idx bs = fastDecodeValue ((schemaTypes schema) !! idx) bytestring
  where
    {-# INLINE bytestring #-}
    bytestring = BS.take (indices !! idx) $ BS.drop startFrom bs
    {-# INLINE startFrom #-}
    startFrom = (length indices) + (sum $ take idx indices)
{-# INLINE decodeFieldByIndex #-}

-- | Decodes a all fields within the record by name
decodeFieldsByName :: [ByteString]
                       -> DbSchema
                       -> (ByteString, ByteString)
                       -> Either DbError (Integer, [DbValue])
decodeFieldsByName flds schema (k, bs) = do
  decodedK      <- decodeKey k
  decodedVal    <- if isJust idxs
                   then mapM (\idx -> decodeFieldByIndex schema (decodeIndexes schema bs) idx bs) (fromJust idxs)
                   else throwError FieldNotFoundError
  return (decodedK, decodedVal)
  where
    {-# INLINE idxs #-}
    idxs         = mapM (`elemIndex` (fields schema)) flds
{-# INLINE decodeFieldsByName #-}

-- | Decodes a single field within the record by name
decodeFieldByName :: ByteString
                     -> DbSchema
                     -> (ByteString, ByteString)
                     -> Either DbError (Integer, DbValue)
decodeFieldByName field !schema !(k, bs) = do
  decodedK      <- decodeKey k
  decodedVal    <- if isJust idx
                   then decodeFieldByIndex schema indices (fromJust idx) bs
                   else throwError FieldNotFoundError
  return $! (decodedK, decodedVal)
  where idx     = elemIndex field (fields schema)
        indices = decodeIndexes schema bs


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

unpackWord64 :: ByteString -> Integer
unpackWord64 s =
    (fromIntegral (s `BS.index` 0) `shiftL` 56) .|.
    (fromIntegral (s `BS.index` 1) `shiftL` 48) .|.
    (fromIntegral (s `BS.index` 2) `shiftL` 40) .|.
    (fromIntegral (s `BS.index` 3) `shiftL` 32) .|.
    (fromIntegral (s `BS.index` 4) `shiftL` 24) .|.
    (fromIntegral (s `BS.index` 5) `shiftL` 16) .|.
    (fromIntegral (s `BS.index` 6) `shiftL`  8) .|.
    (fromIntegral (s `BS.index` 7) )
{-# INLINE unpackWord64 #-}
