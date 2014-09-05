{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Continuum.Tryout where

import           System.Process          (system)
import           Control.Monad.State

import           Continuum.Storage
import           Continuum.Serialization
import           Continuum.Types
import           Continuum.Aggregation
import           Continuum.Folds

import qualified Data.Map as Map

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()

testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)]

main :: IO ()
main = runApp testDBPath testSchema $ do

  liftIO $ cleanup

  putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "a"))]
  putRecord $ makeRecord 128 [("a", (DbInt 2)), ("b", (DbString "a"))]
  putRecord $ makeRecord 129 [("a", (DbInt 3)), ("b", (DbString "b"))]
  putRecord $ makeRecord 130 [("a", (DbInt 4)), ("b", (DbString "d"))]

  -- a <- scan (Just $ encodeBeginTimestamp 126) (withFullRecord id alwaysTrue append) []

  -- a <- aggregateAllByField "b" snd (groupReduce id id (\_ acc -> acc + 1) 1) Map.empty
  -- a <- aggregateAllByField "a" snd (\i acc -> acc + (unpackInt i)) 0

  a <- aggregateAllByField "b" appendFold
  liftIO $ putStrLn $ show a

  -- a <- scanAll2 id (:) []

  -- liftIO $ print (show a)

  liftIO $ cleanup
