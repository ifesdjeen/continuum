{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Continuum.Cluster              ( startNode )
import           Control.Concurrent             ( newEmptyMVar, takeMVar, MVar )

main :: IO ()
main = do
  startedMVar <- newEmptyMVar
  doneMVar    <- newEmptyMVar
  startNode startedMVar doneMVar "/tmp/server-experiments" "5566"
  return ()
