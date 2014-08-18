module Continuum.Actions where

import           Data.Maybe (fromJust)
import qualified Data.Map as Map
import Continuum.Serialization

extractField :: String -> DbRecord -> DbValue
extractField field (DbRecord _ record) = fromJust $ Map.lookup field record

-- minOp :: DbValue -> DbValue -> DbValue
-- minOp (DbInt x) (DbInt y) = DbInt $ min x y
-- minOp _ _ = error ""

-- foldl1 (+) (fmap unpackInt [DbInt 1, DbInt 1])
-- foldl1 min [DbInt 1, DbInt 1]
-- foldl1 max [DbInt 1, DbInt 1]

-- let records = [ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))], makeRecord 124 [("a", (DbInt 2)), ("b", (DbString "2"))], makeRecord 125 [("a", (DbInt 3)), ("b", (DbString "3"))], makeRecord 456 [("a", (DbInt 1)), ("b", (DbString "1"))], makeRecord 456 [("a", (DbInt 2)), ("b", (DbString "2"))], makeRecord 456 [("a", (DbInt 3)), ("b", (DbString "3"))] ]
-- fmap (extractField "a") records

-- foldl1 (+) $ unpackInt <$> (extractField "a") <$> records
