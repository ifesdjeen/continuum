{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import qualified Serialization.Test as Serialization
import qualified Primitive.Test     as Primitive

main :: IO ()
main = do
  Primitive.main
  -- Serialization.main
