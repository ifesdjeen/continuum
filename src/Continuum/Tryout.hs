{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Continuum.Tryout where

import System.Process(system)

-- import Database.LevelDB
-- import Control.Monad.Reader

import Control.Monad.State
import Continuum.Storage
import Continuum.Serialization
import Continuum.Types
import Continuum.Aggregation
import Continuum.Actions
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

  -- a <- aggregateAllByField "b" snd (:) []
  -- liftIO $ putStrLn $ show a

  -- a <- scanAll2 id (:) []

  -- liftIO $ print (show a)

      -- full = indexingEncodeRecord testSchema record 1
      -- encoded = snd $ full
      -- record  = makeRecord 123 [ ("a", (DbInt 123))
      --                          , ("b", (DbString "STRINGIE"))
      --                          , ("c", (DbString "STRINGO"))]
      -- record = encode $ DbPlaceholder 1
      -- indices = decodeIndexes testSchema encoded

  -- liftIO $ print indices

  liftIO $ cleanup

  -- putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
  -- putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
  -- putRecord $ makeRecord 123 [("a", (DbInt 2)), ("b", (DbString "2"))]
  -- putRecord $ makeRecord 123 [("a", (DbInt 3)), ("b", (DbString "3"))]
  -- putRecord $ makeRecord 789 [("a", (DbInt 4)), ("b", (DbString "4"))]

  -- a <- findByTimestamp 123
  -- liftIO $ putStrLn "===== 123 ===== "
  -- liftIO $ putStrLn (show a)



  -- range <- findRange 123 789
  -- liftIO $ putStrLn "===== RANGE ===== "
  -- liftIO $ putStrLn (show range)
  -- liftIO $ putStrLn "===== RANGE ===== "

  -- c <- findAll
  -- liftIO $ putStrLn "===== ALL ===== "
  -- liftIO $ putStrLn (show c)

  -- return a
