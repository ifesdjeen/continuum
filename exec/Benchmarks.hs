{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Data.Map as Map
import           Criterion.Main
import           Data.Serialize as S
import           Data.ByteString (ByteString)
import           Continuum.Types
import           Continuum.Serialization

encoded :: ByteString
encoded = encode ("asd" :: ByteString)

encodedDbValue :: ByteString
encodedDbValue = encode (DbString ("asd" :: ByteString))

main :: IO ()
main = defaultMain [
  -- bench "with pure value"   $ whnf (decode :: ByteString -> Either String ByteString) encoded
  bench "with pure value"   $ whnf (decode :: ByteString -> Either String DbValue)    encodedDbValue
  , bench "with db record " $ whnf (decodeFieldByName "a" testSchema)               (indexingEncodeRecord testSchema testDbRecord 1)
  ]


testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)
                        , ("c", DbtString) ]

testDbRecord =  makeRecord 123 [ ("a", (DbInt 123))
                               , ("b", (DbString "STRINGIE"))
                               , ("c", (DbString "STRINGO"))]
