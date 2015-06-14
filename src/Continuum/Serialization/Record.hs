{-# LANGUAGE BangPatterns #-}

module Continuum.Serialization.Record where

import Continuum.Serialization.Primitive
import Continuum.Serialization.Value

import Control.Monad.Catch  ( MonadMask(..), throwM )
import Data.Serialize       ( runPut, putWord8, putByteString )
import Control.Monad.Except ( forM_ )
import Data.List            ( elemIndex, sort )
import Data.Maybe           ( isJust, fromJust, catMaybes )
import Continuum.Types

import qualified Data.ByteString      as B
import qualified Data.Map.Strict      as Map

encodeRecord :: DbSchema -> Integer -> DbRecord -> Entry
encodeRecord schema sequenceId (DbRecord timestamp vals) = (encodedKey, encodedValue)
  where encodedKey   = B.concat $ fmap packWord64 [timestamp, sequenceId]
        encodedParts = fmap encodeValue $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths      = fmap (\i -> fromIntegral $ B.length i) encodedParts
        encodedValue = B.concat $ (B.pack lengths) : encodedParts

decodeRecord :: (MonadMask m) => Decoding -> DbSchema -> Entry -> m DbRecord
decodeRecord (Field field) schema !(k, bs) = do
  timestamp     <- decodeKey k
  decodedVal    <- if isJust idx
                   then decodeFieldByIndex schema indices (fromJust idx) bs
                   else throwM FieldNotFoundError
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
                   else throwM FieldNotFoundError
  return $! DbRecord timestamp (Map.fromList $ zip flds decodedVals)
  where
    {-# INLINE idxs #-}
    idxs         = mapM (`elemIndex` (fields schema)) flds
{-# INLINE decodeRecord #-}



-- |
-- | Utility Functions
-- |

decodeKey :: (MonadMask m) => DbKey -> m Integer
decodeKey = unpackWord64
{-# INLINE decodeKey #-}

decodeIndexes :: DbSchema -> EncodedValue -> [Int]
decodeIndexes schema bs =
  map fromIntegral $ B.unpack $ B.take (length (fields schema)) bs
{-# INLINE decodeIndexes #-}

-- | Decodes field by index
decodeFieldByIndex :: (MonadMask m) =>
                      DbSchema
                      -> [Int]
                      -> Int
                      -> EncodedValue
                      -> m DbValue
decodeFieldByIndex schema indices idx bs = decodeValue ((schemaTypes schema) !! idx) bytestring
  where
    {-# INLINE bytestring #-}
    bytestring = B.take (indices !! idx) $ B.drop startFrom bs
    {-# INLINE startFrom #-}
    startFrom = (length indices) + (sum $ take idx indices)
{-# INLINE decodeFieldByIndex #-}

decodeValues :: (MonadMask m) => DbSchema -> EncodedValue -> m [DbValue]
decodeValues schema bs = mapM (\(t, s) -> decodeValue t s) (zip (schemaTypes schema) bytestrings)
  where
    indices                  = decodeIndexes schema bs
    bytestrings              = snd (foldl step (B.drop (length indices) bs, []) indices)
    step (remaining', acc) n = (B.drop n remaining', acc ++ [B.take n remaining'])
{-# INLINE decodeValues #-}

getValue :: FieldName -> DbRecord -> Maybe DbValue
getValue fieldName (DbRecord _ recordFields) = Map.lookup fieldName recordFields

-- getValue :: (MonadMask m) => FieldName -> DbRecord -> m DbValue
-- getValue fieldName (DbRecord _ recordFields) =
--   case (Map.lookup fieldName recordFields) of
--     (Just f) -> return f
--     Nothing ->  throwM FieldNotFoundError
