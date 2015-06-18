{-# LANGUAGE OverloadedStrings #-}

module Main where

import Continuum.Service.HttpService

main :: IO ()
main = do
  runWebServer
