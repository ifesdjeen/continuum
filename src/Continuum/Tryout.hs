{-# LANGUAGE FlexibleContexts #-}

module Continuum.Tryout where

import System.Process(system)

-- import Database.LevelDB
-- import Control.Monad.Reader

import Control.Monad.State
import Continuum.Storage
import Continuum.Serialization

import qualified Data.Map as Map

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()

main :: IO ()
main = runApp testDBPath $ do

  liftIO $ cleanup

  putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
  putRecord $ makeRecord 123 [("a", (DbInt 1)), ("b", (DbString "1"))]
  putRecord $ makeRecord 123 [("a", (DbInt 2)), ("b", (DbString "2"))]
  putRecord $ makeRecord 123 [("a", (DbInt 3)), ("b", (DbString "3"))]
  putRecord $ makeRecord 789 [("a", (DbInt 4)), ("b", (DbString "4"))]

  a <- findTs 123
  liftIO $ putStrLn "===== 123 ===== "
  liftIO $ putStrLn (show a)

  range <- findRange 123 789
  liftIO $ putStrLn "===== RANGE ===== "
  liftIO $ putStrLn (show range)
  liftIO $ putStrLn "===== RANGE ===== "

  c <- findAll
  liftIO $ putStrLn "===== ALL ===== "
  liftIO $ putStrLn (show c)

  -- return a
