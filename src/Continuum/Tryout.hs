{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Continuum.Tryout where

import           Control.Monad.State
import           Data.ByteString (ByteString)
import           Continuum.Storage
import           Continuum.Serialization
import           Continuum.Types
-- import           Continuum.Aggregation
import           Continuum.Folds
-- import qualified Data.Map as Map

testDbPath :: String
testDbPath = "/tmp/haskell-leveldb-tests5"

testDbName :: ByteString
testDbName = "testdb"

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)]

main :: IO (Either DbError [DbResult])
main = runApp testDbPath testSchema $ do

  createDatabase testDbName testSchema

  putRecord testDbName $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "a"))]
  putRecord testDbName $ makeRecord 128 [("a", (DbInt 2)), ("b", (DbString "a"))]

  a <- scan testDbName (TsSingleKey 123)    Record appendFold
  a <- scan testDbName (TsOpenEnd 123)      Record appendFold
  a <- scan testDbName (TsKeyRange 123 300) Record appendFold

  return a
  -- a <- scanAll2 id (:) []
