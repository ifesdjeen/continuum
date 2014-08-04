{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}

module Continuum.Serialization where

-- import           Control.Monad (liftM)
import qualified Data.Map as Map
-- import           Data.Serialize (Serialize, encode, decode)
import           Data.Serialize as S
import           GHC.Generics
import           Data.Maybe
import qualified Data.ByteString as B

data Success = Success

data DbType = DbtInt | DbtString

data DbValue = DbInt Int
             | DbString String
             | DbTimestamp Integer
             | DbList [DbValue]
             | DbMap [(DbValue, DbValue)]
             deriving (Show, Eq, Ord, Generic)

instance Serialize DbValue

data DbRecord = DbRecord DbValue DbValue (Map.Map String DbValue)

data DbSchema = DbSchema { fieldMappings :: (Map.Map String Int)
                           , fields :: [String]
                           , indexMappings :: (Map.Map Int String)
                           , schemaMappings :: (Map.Map String DbType) }

makeSchema :: [(String, DbType)] -> DbSchema
makeSchema stringTypeList = DbSchema { fieldMappings = fMappings
                                     , fields = fields'
                                     , schemaMappings = Map.fromList stringTypeList
                                     , indexMappings = iMappings }
  where fields' = fmap fst stringTypeList
        fMappings = Map.fromList $ zip fields' iterateFrom0
        iMappings = Map.fromList $ zip iterateFrom0 fields'
        iterateFrom0 = (iterate (1+) 0)



validate :: DbSchema -> DbRecord -> Either String Success
validate = error "Not Implemented"

-- makeDbRecord :: (B.ByteString, B.ByteString) -> DbRecord
makeDbRecord :: DbSchema ->  (B.ByteString, B.ByteString) -> DbRecord
makeDbRecord schema (k, v) = DbRecord timestamp sequenceId (Map.fromList $ zip (fields schema) values) -- values
                          where (timestamp, sequenceId) = fromRightTuple $ decode k
                                values = fromRightList $ decode v

makeDbRecords :: DbSchema -> [(B.ByteString, B.ByteString)] -> [DbRecord]
makeDbRecords schema items = fmap (makeDbRecord schema) items
-- makeDbRecords = error "asd"

-- throws an error if its argument take the form  @Left _@.
fromRightTuple           :: Either String (DbValue, DbValue) -> (DbValue, DbValue)
fromRightTuple (Left a)  = error a
fromRightTuple (Right x) = x

fromRightList           :: Either String [DbValue] -> [DbValue]
fromRightList (Left a)  = error a
fromRightList (Right x) = x

-- makeDbRecord schema k v = DbRecord timestamp sequenceId [] -- values
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
