{-# LANGUAGE BangPatterns #-}

module Continuum.Serialization.Record where

import qualified Data.ByteString      as B


import Continuum.Serialization.Primitive
import Continuum.Serialization.Value

import Control.Exception (throw)
import Control.Monad.Catch    (MonadMask)
import           Data.Serialize       ( runPut, putWord8, putByteString )
import           Control.Monad.Except ( forM_, throwError )
import           Data.List            ( elemIndex )
import           Data.Maybe           ( isJust, fromJust, catMaybes )
import           Continuum.Types
import qualified Data.Map             as Map

encodeRecord :: DbSchema -> Integer -> DbRecord -> Entry
encodeRecord schema sequenceId (DbRecord timestamp vals) = (encodedKey, encodedValue)
  where encodedKey = B.concat [(packWord64 timestamp), (packWord64 sequenceId)]
        encodedParts = fmap encodeValue $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths = B.length <$> encodedParts
        encodedValue = runPut $ do
          -- Change to B.Pack
          forM_ lengths (putWord8 . fromIntegral)
          forM_ encodedParts putByteString
          -- encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

decodeRecord :: Decoding -> DbSchema -> Decoder DbRecord
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
-- | Utility Functions
-- |

decodeKey :: DbKey -> DbErrorMonad Integer
decodeKey = unpackWord64
{-# INLINE decodeKey #-}

decodeIndexes :: DbSchema -> EncodedValue -> [Int]
decodeIndexes schema bs =
  map fromIntegral $ B.unpack $ B.take (length (fields schema)) bs
{-# INLINE decodeIndexes #-}

-- | Decodes field by index
decodeFieldByIndex :: DbSchema
                      -> [Int]
                      -> Int
                      -> EncodedValue
                      -> DbErrorMonad DbValue
decodeFieldByIndex schema indices idx bs = decodeValue ((schemaTypes schema) !! idx) bytestring
  where
    {-# INLINE bytestring #-}
    bytestring = B.take (indices !! idx) $ B.drop startFrom bs
    {-# INLINE startFrom #-}
    startFrom = (length indices) + (sum $ take idx indices)
{-# INLINE decodeFieldByIndex #-}

decodeValues :: DbSchema -> EncodedValue -> DbErrorMonad [DbValue]
decodeValues schema bs = mapM (\(t, s) -> decodeValue t s) (zip (schemaTypes schema) bytestrings)
  where
    indices                  = decodeIndexes schema bs
    bytestrings              = snd (foldl step (B.drop (length indices) bs, []) indices)
    step (remaining', acc) n = (B.drop n remaining', acc ++ [B.take n remaining'])
{-# INLINE decodeValues #-}

-- |
-- | Construction
-- |

-- | Creates a DbRecord from Timestamp and Key/Value pairs
--
makeRecord :: Integer -> [(FieldName, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

getValue :: FieldName -> DbRecord -> Maybe DbValue
getValue fieldName (DbRecord _ recordFields) = Map.lookup fieldName recordFields

getValue' :: FieldName -> DbRecord -> DbErrorMonad DbValue
getValue' fieldName (DbRecord _ recordFields) =
  case (Map.lookup fieldName recordFields) of
    (Just f) -> Right f
    Nothing ->  Left FieldNotFoundError
