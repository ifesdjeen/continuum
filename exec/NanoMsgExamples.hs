{-# LANGUAGE OverloadedStrings #-}

module Main where

import Continuum.Cluster

main :: IO ()
main = startNode

-- main =
--   nLat 40 1000 "tcp://*:5566" "tcp://127.0.0.1:5566"
