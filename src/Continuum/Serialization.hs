{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Serialization where
import           Control.Applicative ((<$>))
-- import           GHC.Word (Word8)

import qualified Data.Map as Map

import           Continuum.Types
import           Control.Arrow
import           Data.List (elemIndex)
import           GHC.Generics(Generic)

import           Data.Serialize as S
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS

import           Data.Maybe (isJust, fromJust, catMaybes)
import           Control.Monad.Except(forM_, forM, throwError)

-- import Debug.Trace

data Success = Success

-- data DbTimestamp = Integer
--                  deriving (Show, Eq, Ord, Generic)

-- type DbTimestamp = Integer
-- type DbSequenceId = Maybe Integer
-- newtype DbSequenceId = DbSequenceId (Maybe Integer)
--                      deriving (Show, Eq, Ord, Generic)

-- data DbSequenceId = DbSequenceId (Maybe Integer)
--                   deriving (Show, Eq, Ord, Generic)

data DbValue = DbInt Integer
             | DbFloat Float
             | DbDouble Double
             | DbString ByteString
             | DbTimestamp Integer
             | DbSequenceId Integer
             -- | DbList [DbValue]
             -- | DbMap [(DbValue, DbValue)]
             deriving (Show, Eq, Ord, Generic)

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

tsPlaceholder :: ByteString
tsPlaceholder = encode (DbString "____placeholder" :: DbValue)

instance Serialize DbValue
instance Serialize DbError


-- TODO: change String to ByteString
-- overloadedstrings
data DbRecord = DbRecord Integer (Map.Map ByteString DbValue) |
                DbPlaceholder Integer
                deriving(Show, Eq)

-- (Ord key, Eq val) =>
data Group key val = Group [(key, [val])]
                   | GroupAggregate [(key, val)]
                   deriving (Show, Eq, Ord)



makeRecord :: Integer -> [(ByteString, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

validate :: DbSchema -> DbRecord -> Either String Success
validate = error "Not Implemented"

removePlaceholder :: DbRecord -> Bool
removePlaceholder (DbRecord _ _) = True
removePlaceholder (DbPlaceholder _) = False

encodeRecord :: DbSchema -> DbRecord -> Integer -> (ByteString, ByteString)
encodeRecord schema (DbRecord timestamp vals) sid = (encodeKey, encodeValue)
  where encodeKey = encode (timestamp, sid)
        encodeValue = encode . catMaybes $ fmap (`Map.lookup` vals) (fields schema)

encodeRecord _ (DbPlaceholder timestamp) sid = (encodeKey, encodeValue)
  where encodeKey = encode (timestamp, sid)
        encodeValue = tsPlaceholder

-- | Decode a single value (mostly a wrapper over @decode, giving a more concrete
-- Error type)
decodeValue :: ByteString -> Either DbError DbValue
decodeValue x = left ValueDecodeError $ decode x

-- | Decode a single key (mostly a wrapper over @decode, giving a more concrete
-- Error type)
decodeKey :: ByteString -> Either DbError (Integer, Integer)
decodeKey x = left KeyDecodeError $ decode x

decodeRecord :: DbSchema ->  (ByteString, ByteString) -> Either DbError DbRecord
decodeRecord schema (k, v) = do
  (timestamp, _) <- decodeKey k
  decodedValue   <- decodeValues schema v
  return $ DbRecord timestamp (Map.fromList $ zip (fields schema) decodedValue)

-- unwrapRecord :: Either String DbRecord -> DbRecord
-- unwrapRecord (Right x) = x

encodeBeginTimestamp :: Integer -> ByteString
encodeBeginTimestamp timestamp = encode (timestamp, 0 :: Integer)

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

append :: a -> [a] -> [a]
append val acc = acc ++ [val]

instance (Ord a) => Functor (Group a) where
  fmap f (Group vals) = Group $ fmap mapEntries vals
    where mapEntries (k, v) = (k, fmap f v)

foldGroup :: (b -> c -> c) -> c -> Group a b -> Group a c
foldGroup f acc (Group vals) = GroupAggregate $ fmap foldEntries vals
                             where foldEntries (k, v) = (k, Prelude.foldr f acc v)

foldGroup1 :: (val -> val -> val) -> Group key val -> Group key val
foldGroup1 f (Group vals) = GroupAggregate $ fmap foldEntries vals
                            where foldEntries (k, v) = (k, Prelude.foldr1 f v)

foldTuple :: (b -> c -> c) -> c -> (a, [b]) -> (a, c)
foldTuple f acc (k, coll) = (k, Prelude.foldr f acc coll)

byField :: ByteString -> DbRecord -> DbValue
byField f (DbRecord _ m) = fromJust $ Map.lookup f m
byField _ _ = DbInt 1 -- WTF

byFieldMaybe :: ByteString -> DbRecord -> Maybe DbValue
byFieldMaybe f (DbRecord _ m) = Map.lookup f m
byFieldMaybe _ _ = Just $ DbInt 1 -- WTF

byTime :: Integer -> DbRecord -> Integer
byTime interval (DbRecord t _) = interval * (t `quot` interval)

-- Add multi-groups for grouping via multiple fields / preds

indexingEncodeRecord :: DbSchema -> DbRecord -> Integer -> (ByteString, ByteString)
indexingEncodeRecord schema (DbRecord timestamp vals) sid = (encodeKey, encodeValue)
  where encodeKey = encode (timestamp, sid)
        -- encodedVals = fmap encode $ catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)
        encodedParts = fmap encode $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths = BS.length <$> encodedParts
        encodeValue = runPut $ do
          forM_ lengths (putWord8 . fromIntegral)
          forM_ encodedParts putByteString
          -- encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

decodeIndexes :: DbSchema -> ByteString -> Either DbError [Int]
decodeIndexes schema bs = case decodeIndexes' bs of
                            (Left a) -> throwError $ IndexesDecodeError a
                            (Right x) -> Right $ map fromIntegral x
                       where decodeIndexes' = runGet $ forM (fields schema) (\_ -> getWord8)

decodeValues :: DbSchema -> ByteString -> Either DbError [DbValue]
decodeValues schema bs = do x <- left ValuesDecodeError $ decodeAll bs
                            mapM decodeValue x
                          where decodeAll = runGet $ do idx <- forM (fields schema) (\_ -> getWord8)
                                                        forM idx (\c -> getBytes (fromIntegral c))

decodeFrom :: Int -> ByteString -> Either String DbValue
decodeFrom from bs = case read bs of
                          (Left a)  -> error a
                          (Right x) -> decode x
  where read = runGet $ do uncheckedSkip from
                           rem <- remaining
                           getBytes rem

decodeFieldByIndex :: Either DbError [Int] -> Int -> ByteString -> Either DbError DbValue
decodeFieldByIndex eitherIndices idx bs = eitherIndices >>= read'
  where read' indices = case read indices bs of
          (Left a)  -> throwError $ DecodeFieldByIndexError a indices
          (Right x) -> decodeValue x
        read indices = runGet $ do uncheckedSkip (beginIdx + length indices)
                                   getBytes $ indices !! idx
                       where beginIdx = sum $ take idx indices

-- GetField typeclass???? For bytestings
getFieldByName :: ByteString -> DbRecord -> Either DbError DbValue
getFieldByName field (DbRecord _ values) = if isJust fieldVal
                                           then return $ fromJust fieldVal
                                           else throwError FieldNotFoundError
  where fieldVal = Map.lookup field values

decodeFieldByName :: ByteString -> DbSchema -> (ByteString, ByteString) -> Either DbError DbValue
decodeFieldByName field schema (_, bs) = if isJust idx
                                            then decodeFieldByIndex indices (fromJust idx) bs
                                            else throwError FieldNotFoundError
  where idx = elemIndex field (fields schema)
        indices = decodeIndexes schema bs

decodeFieldsByName :: [ByteString] -> DbSchema -> (ByteString, ByteString) -> Either DbError [DbValue]
decodeFieldsByName flds schema (_, bs) = if isJust idxs
                                         then mapM (\idx -> decodeFieldByIndex indices idx bs) (fromJust idxs)
                                         else throwError FieldNotFoundError
  where idxs = mapM (`elemIndex` (fields schema)) flds
        indices = decodeIndexes schema bs
