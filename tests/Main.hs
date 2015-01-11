{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import qualified Storage.Test      as Storage
import qualified Server.Test       as Server

main :: IO ()
main = do
  Storage.main
  -- Server.main
