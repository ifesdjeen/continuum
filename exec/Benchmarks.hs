{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Criterion.Main
import           Continuum.Types
import           Continuum.Serialization

main :: IO ()
main = defaultMain [
  -- bench "with pure value"   $ whnf (decode :: ByteString -> Either String ByteString) encoded
  bench "with db record " $ whnf (decodeRecord (Field "b") testSchema)               (encodeRecord testSchema testDbRecord 1)
  ]


testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)
                        , ("c", DbtString) ]

testDbRecord =  makeRecord 123 [ ("a", (DbInt 123))
                               , ("b", (DbString "STRINGIE"))
                               , ("c", (DbString "STRINGO"))]
