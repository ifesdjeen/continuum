{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Continuum.Serialization where
import           Control.Applicative ((<$>), (<*>))
import           Control.Monad (forM_, forM)
import           GHC.Word (Word8)
-- import           Control.Monad (liftM)
import qualified Data.Map as Map
-- import           Data.Serialize (Serialize, encode, decode)
import           Data.Serialize as S
import           GHC.Generics
import qualified Data.ByteString as B
import           Data.Maybe (fromMaybe, fromJust, catMaybes)
import           Data.Serialize.Get (Get)
import           Data.Serialize.Put (Put)
import           Control.Monad.Error

import Control.Monad.IO.Class (MonadIO (liftIO))

import Debug.Trace

data DbError = IndexesDecodeError String |
               FieldDecodeError String B.ByteString |
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

decodeRecord' :: DbSchema -> (Integer, Integer) -> B.ByteString -> DbRecord
decodeRecord' _ (timestamp, 0) _ =
  DbPlaceholder timestamp

decodeRecord' schema (timestamp, _) v =
  DbRecord timestamp (Map.fromList $ zip (fields schema) values) -- values
  where values = decodeValue v

decodeRecord :: DbSchema ->  (B.ByteString, B.ByteString) -> DbRecord
decodeRecord schema (k, v) =
  decodeRecord' schema (decodeKey k) v

decodeRecords :: DbSchema -> [(B.ByteString, B.ByteString)] -> [DbRecord]
decodeRecords schema items = filter removePlaceholder (fmap (decodeRecord schema) items)
-- decodeRecords = error "asd"

decodeKey :: B.ByteString -> (Integer, Integer)
decodeKey k = case decode k of
              (Left a)  -> error a
              (Right x) -> x

decodeValue :: B.ByteString -> [DbValue]
decodeValue k = case decode k of
              (Left a)  -> error a
              (Right x) -> x

-- decodeRecord schema k v = DbRecord timestamp sequenceId [] -- values
--                           where (timestamp, sequenceId) = decode k :: Either String DbValue
--                                 values = decode v :: Either String DbValue
-- decode (encode $ DbString "asdasdasdasdasdasdasdasdasdasdasdasd") :: Either String DbValue


-- decode (encode $ [DbString "abc", DbString "cde", DbInt 1]) :: Either String DbValue

-- serializeDbRecord :: Schema




-- instance Serialize [(String, DbValue)] where
--   encode [] = B.empty
--   encode [(_, v):xs] = (encode v) ++ (encode xs)

-- instance Serialize DbValue where
--   put (DbString s) = put ((B.length bs), bs)
--     where bs = (encode s)



--- We have a map, but problem with maps is random key order
--- how do we fix that?
--- We need to take a hashmap and record order?
--- Basically, everything should be done through the lists??
--- decode (encode $ [(DbString "asd"), (DbInt 1)]) :: Either String [DbValue]
--- How to construct these lists though? We take schema, schema has index mappings


-- instance Serialize [(String, DbValue)] where
--   encode [] = B.empty
--   encode [(_, v):xs] = (encode v) ++ (encode xs)

-- encodeValues :: DbRecord -> ByteString
-- encodeValues record = foldl serializeOne B.empty

-- encoding values takes schema and returns a serialized value


-- Map.fromList [("key1", (DbInt 1)), ("key2", (DbString "asd"))]


-- Chec

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

append :: DbRecord -> [DbRecord] -> [DbRecord]
append val@(DbRecord _ _) acc = acc ++ [val]
append (DbPlaceholder _) acc = acc

instance (Ord a) => Functor (Group a) where
  fmap f (Group vals) = Group $ fmap mapEntries vals
    where mapEntries (k, v) = (k, fmap f v)

-- (b -> c) -> [(a, [b])] -> [(a, [c])]
-- (a -> b -> b) -> b -> [a] -> b

foldGroup :: (b -> c -> c) -> c -> Group a b -> Group a c
foldGroup f acc (Group vals) = GroupAggregate $ fmap foldEntries vals
                             where foldEntries (k, v) = (k, Prelude.foldr f acc v)

foldGroup1 :: (val -> val -> val) -> Group key val -> Group key val
foldGroup1 f (Group vals) = GroupAggregate $ fmap foldEntries vals
                            where foldEntries (k, v) = (k, Prelude.foldr1 f v)

foldTuple :: (b -> c -> c) -> c -> (a, [b]) -> (a, c)
foldTuple f acc (k, coll) = (k, Prelude.foldr f acc coll)

-- sortAndGroup assocs = fromListWith (++) [(k, [v]) | (k, v) <- assocs]

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
                                   rem' <- remaining

                                   a <- trace ((show bs) ++ "  " ++
                                               (show indices) ++ "  " ++
                                               (show $ (beginIdx + length indices)) ++ "  " ++
                                               (show $ fromMaybe rem' toTake) ++ "  "

                                               -- (show $ bs) ++
                                               ) (return $ fromMaybe rem' toTake)

                                   getBytes $ a
                       where beginIdx = sum $ take idx indices
                             toTake = if idx + 1 > length indices
                                      then Nothing
                                      -- OH MY THATS JUST SAME AS IF WE TOOK THAT INDEX WITH !!
                                      else Just $ sum (take (idx+1) indices) - beginIdx

tryIndexEncode = do
  print $ snd encoded
  print $ decodeIndexes sch (snd encoded)

  let indices = decodeIndexes sch (snd encoded)

  print $ decodeFieldByIndex indices 0 (snd encoded)
  print $ decodeFieldByIndex indices 1 (snd encoded)
  print $ decodeFieldByIndex indices 2 (snd encoded)

  -- let (Right [idx1, idx2, idx3]) = decodeIndexes sch (snd encoded)
  -- forM_ [0..20] (\i -> putStrLn $ show $ decodeFrom (6 + 17 + i) (snd encoded))
  -- print $ decodeFrom (6 + 17 + 3) (snd encoded)
  -- 3 is just a number of Words i've put before that thing :) lol

  -- decodeIndexes sch (snd encoded)
  -- B.length (snd encoded)
  -- decodeFrom 5 (snd encoded)
  where encoded = indexingEncodeRecord sch record 1
        sch = makeSchema [ ("a", DbtInt)
                         , ("b", DbtString)
                         , ("c", DbtString)]
        record  = makeRecord 123 [("a", (DbInt 123))
                                 , ("b", (DbString "STRINGIE"))
                                 , ("c", (DbString "STRINGO"))]
