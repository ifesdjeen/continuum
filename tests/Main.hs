{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import qualified Serialization.Test as Storage
import qualified Storage.Test as Serialization

main :: IO ()
main = do
  Storage.main
  Serialization.main
