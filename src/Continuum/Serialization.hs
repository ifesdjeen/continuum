{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Serialization where
import           Control.Applicative ((<$>))
-- import           GHC.Word (Word8)

import qualified Data.Map as Map

import           Data.List (elemIndex)
import           GHC.Generics(Generic)

import           Data.Serialize as S
import qualified Data.ByteString as B

import           Data.Maybe (fromMaybe, isJust, fromJust, catMaybes)
import           Data.Either (rights)
-- import           Data.Serialize.Get (Get)
-- import           Data.Serialize.Put (Put)
import           Control.Monad.Except(forM_, forM, throwError, sequence)

-- import Control.Monad.IO.Class (MonadIO (liftIO))

import Debug.Trace

data DbError = IndexesDecodeError String |
               FieldDecodeError String B.ByteString |
               FieldNotFoundError |
               DecodeFieldByIndexError String [Int] |
               OtherError
             deriving (Show, Eq, Ord, Generic)

data Success = Success

data DbType = DbtInt | DbtString

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
             | DbString String
             | DbTimestamp Integer
             | DbSequenceId Integer
             | DbList [DbValue]
             | DbMap [(DbValue, DbValue)]
             deriving (Show, Eq, Ord, Generic)

unpackString :: DbValue -> String
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

-- instance Serialize DbTimestamp
-- instance Serialize DbSequenceId
instance Serialize DbValue
instance Serialize DbError

-- change String to ByteString
-- overloadedstrings
data DbRecord = DbRecord Integer (Map.Map String DbValue) |
                DbPlaceholder Integer
                deriving(Show, Eq)

-- (Ord key, Eq val) =>
data Group key val = Group [(key, [val])]
                   | GroupAggregate [(key, val)]
                   deriving (Show, Eq, Ord)

data DbSchema = DbSchema { fieldMappings    :: Map.Map String Int
                           , fields         :: [String]
                           , indexMappings  :: Map.Map Int String
                           , schemaMappings :: Map.Map String DbType }

makeSchema :: [(String, DbType)] -> DbSchema

makeSchema stringTypeList = DbSchema { fieldMappings = fMappings
                                     , fields = fields'
                                     , schemaMappings = Map.fromList stringTypeList
                                     , indexMappings = iMappings }
  where fields' = fmap fst stringTypeList
        fMappings = Map.fromList $ zip fields' iterateFrom0
        iMappings = Map.fromList $ zip iterateFrom0 fields'
        iterateFrom0 = (iterate (1+) 0)


makeRecord :: Integer -> [(String, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

validate :: DbSchema -> DbRecord -> Either String Success
validate = error "Not Implemented"

removePlaceholder :: DbRecord -> Bool
removePlaceholder (DbRecord _ _) = True
removePlaceholder (DbPlaceholder _) = False

encodeRecord :: DbSchema -> DbRecord -> Integer -> (B.ByteString, B.ByteString)
encodeRecord schema (DbRecord timestamp vals) sid = (encodeKey, encodeValue)
  where encodeKey = encode (timestamp, sid)
        encodeValue = encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

decodeRecord :: DbSchema ->  (B.ByteString, B.ByteString) -> Either String DbRecord
decodeRecord schema (k, v) = do
  (timestamp, _) <- decode k :: Either String (Integer, Integer)
  decodedValue   <- decodeValues schema v
  return $ DbRecord timestamp (Map.fromList $ zip (fields schema) decodedValue)

unwrapRecord :: Either String DbRecord -> DbRecord
unwrapRecord (Right x) = x

encodeBeginTimestamp :: Integer -> B.ByteString
encodeBeginTimestamp timestamp = encode (timestamp, 0 :: Integer)

-- isSameTimestamp :: Integer -> DbRecord -> a -> Bool
-- isSameTimestamp begin (DbRecord current _) _ = begin == current
-- isSameTimestamp begin (DbPlaceholder current) _ = begin == current
-- isSameTimestamp begin _ _ = False


compareTimestamps :: (Integer -> Integer -> Bool)
                     -> Integer
                     -> DbRecord
                     -> a
                     -> Bool

compareTimestamps op begin (DbRecord current _) _ = current `op` begin
compareTimestamps op begin (DbPlaceholder current) _ = current `op` begin
compareTimestamps _  _ _ _ = False

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

byField :: String -> DbRecord -> DbValue
byField f (DbRecord _ m) = fromJust $ Map.lookup f m
byField _ _ = DbInt 1 -- WTF

byFieldMaybe :: String -> DbRecord -> Maybe DbValue
byFieldMaybe f (DbRecord _ m) = Map.lookup f m
byFieldMaybe _ _ = Just $ DbInt 1 -- WTF

byTime :: Integer -> DbRecord -> Integer
byTime interval (DbRecord t _) = interval * (t `quot` interval)

-- Add multi-groups for grouping via multiple fields / preds

indexingEncodeRecord :: DbSchema -> DbRecord -> Integer -> (B.ByteString, B.ByteString)
indexingEncodeRecord schema (DbRecord timestamp vals) sid = (encodeKey, encodeValue)
  where encodeKey = encode (timestamp, sid)
        -- encodedVals = fmap encode $ catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)
        encodedParts = fmap encode $ catMaybes $ (\x -> Map.lookup x vals) <$> (fields schema)
        lengths = B.length <$> encodedParts
        encodeValue = runPut $ do
          forM_ lengths (putWord8 . fromIntegral)
          forM_ encodedParts putByteString
          -- encode . catMaybes $ fmap (\x -> Map.lookup x vals) (fields schema)

decodeIndexes :: DbSchema -> B.ByteString -> Either DbError [Int]
decodeIndexes schema bs = case decodeIndexes' bs of
                            (Left a) -> throwError $ IndexesDecodeError a
                            (Right x) -> Right $ map fromIntegral x
                       where decodeIndexes' = runGet $ forM (fields schema) (\_ -> getWord8)


decodeValues :: DbSchema -> B.ByteString -> Either String [DbValue]
decodeValues schema bs = do x <- decodeAll bs
                            sequence $ (map decode x)
                          where decodeAll = runGet $ do idx <- forM (fields schema) (\_ -> getWord8)
                                                        forM idx (\c -> getBytes (fromIntegral c))

decodeFrom :: Int -> B.ByteString -> Either String DbValue
decodeFrom from bs = case read bs of
                          (Left a)  -> error a
                          (Right x) -> decode x
  where read = runGet $ do uncheckedSkip from
                           rem <- remaining
                           getBytes rem

wrapDecode :: B.ByteString -> Either DbError DbValue
wrapDecode a =
  case decode a of
    (Left err)  -> throwError $ FieldDecodeError err a
    (Right x) -> return x

decodeFieldByIndex :: Either DbError [Int] -> Int -> B.ByteString -> Either DbError DbValue
decodeFieldByIndex eitherIndices idx bs = eitherIndices >>= read'
  where read' indices = case read indices bs of
          (Left a)  -> throwError $ DecodeFieldByIndexError a indices
          (Right x) -> wrapDecode x
        read indices = runGet $ do uncheckedSkip (beginIdx + length indices)

                                   -- let a = trace (show $ indices) (indices !! idx)

                                   getBytes $ indices !! idx
                       where beginIdx = sum $ take idx indices

decodeFieldByName :: String -> DbSchema -> (B.ByteString, B.ByteString) -> Either DbError DbValue
decodeFieldByName field schema (_, bs) = if isJust idx
                                            then decodeFieldByIndex indices (fromJust idx) bs
                                            else throwError FieldNotFoundError
  where idx = elemIndex field (fields schema)
        indices = decodeIndexes schema bs
