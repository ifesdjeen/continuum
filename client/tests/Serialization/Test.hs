{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Serialization.Test where

import Continuum.Common.Serialization
import Continuum.Common.Types
import Test.Hspec

import Debug.Trace

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)
                        , ("c", DbtString) ]
main :: IO ()
main =  hspec $ do

  describe "Serialization" $ do

    let record  = makeRecord 123 [ ("a", (DbInt 123))
                                 , ("b", (DbString "STRINGIE"))
                                 , ("c", (DbString "STRINGO"))]
        encoded = encodeRecord testSchema record 1
        indices = decodeIndexes testSchema (snd encoded)

    it "reads out indexes from serialized items" $
      indices `shouldBe` [4,8,7]

    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeFieldByIndex testSchema indices x (snd encoded)
      decodeFn 0 `shouldBe` (Right $ DbInt    123)
      decodeFn 1 `shouldBe` (Right $ DbString "STRINGIE")
      decodeFn 2 `shouldBe` (Right $ DbString "STRINGO")

    it "decodes certain serialized values" $ do
      let decodeFn = \x -> decodeRecord (Field x) testSchema encoded
      decodeFn "a" `shouldBe` (Right $ makeRecord 123 [ ("a", (DbInt 123))])

    it "decodes a complete serialized value" $ do
      let decodeFn = \x -> decodeRecord Record testSchema encoded
      decodeFn "a" `shouldBe` (Right record)

  describe "Partial Serialization" $ do
    -- OKAY HERES THE BUG
    it "decodes the record that was only partially encoded" $ do
      let schema = makeSchema [ ("a", DbtString)
                              , ("b", DbtLong)
                              , ("c", DbtLong) ]
          record  = makeRecord 123 [("a", (DbString "STRINGIE"))]
          encoded = encodeRecord schema record 1
          decodeFn = \x -> decodeRecord Record schema encoded
      decodeFn "a" `shouldBe` (Right record)
