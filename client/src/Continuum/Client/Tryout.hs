{-# LANGUAGE OverloadedStrings #-}

module Main where

import Continuum.Client.Base
import qualified Data.Map                            as Map
memorySchema =
  makeSchema [ ("host",    DbtString)
             , ("value",   DbtInt)
             , ("subtype", DbtString) ]

main = do
  client <- connect "127.0.0.1" "5566"
  res    <- executeQuery client (FetchAll "memory")
  -- res      <- executeQuery client (CreateDb "memory" memorySchema)
  print $  res
  -- res      <- executeQuery client (DbRecord 1416069569803 (Map.fromList [("host",DbString "127.0.0.1"),("subtype",DbString "wired"),("value",DbInt 2082492416)]))
  return ()
