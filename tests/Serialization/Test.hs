{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Serialization.Test where

import Continuum.Common.Serialization
import Continuum.Types
import Test.Hspec

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)
                        , ("c", DbtString) ]
main :: IO ()
main =  hspec $ do

  describe "Serialization" $ do

    let encoded = encodeRecord testSchema record 1
        record  = makeRecord 123 [ ("a", (DbInt 123))
                                 , ("b", (DbString "STRINGIE"))
                                 , ("c", (DbString "STRINGO"))]
        indices = decodeIndexes testSchema (snd encoded)
    it "reads out indexes from serialized items" $
      indices `shouldBe` [8,8,7]

    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeFieldByIndex testSchema indices x (snd encoded)
      decodeFn 0 `shouldBe` (Right $ DbInt    123)
      decodeFn 1 `shouldBe` (Right $ DbString "STRINGIE")
      decodeFn 2 `shouldBe` (Right $ DbString "STRINGO")


    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeRecord (Field x) testSchema encoded
      decodeFn "a" `shouldBe` (Right $ FieldRes (123, DbInt    123))
      decodeFn "b" `shouldBe` (Right $ FieldRes (123, DbString "STRINGIE"))
      decodeFn "c" `shouldBe` (Right $ FieldRes (123, DbString "STRINGO"))
