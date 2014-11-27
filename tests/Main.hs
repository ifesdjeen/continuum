{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import qualified Storage.Test       as Storage
import qualified Cluster.Test       as Cluster

main :: IO ()
main = do
  Storage.main
  Cluster.main
