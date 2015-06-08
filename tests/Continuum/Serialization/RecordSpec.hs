{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

{-# OPTIONS_GHC -fno-warn-orphans            #-}

module Continuum.Serialization.RecordSpec where

import Continuum.Serialization.Record
import Continuum.Types

import Continuum.Support.QuickCheck

import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Monadic

sid :: Integer
sid = 456

prop_roundTrip :: (DbSchema, DbRecord) -> Property
prop_roundTrip (schema, record) = monadicIO $ do
  let encoded = encodeRecord schema sid record
      decoder = decodeRecord Record schema
  result <- run $ decoder encoded
  assert $ record == result

spec :: Spec
spec = do

  describe "Property Test" $ do
    it "passes Round Trip" $ do
      property $ prop_roundTrip

  describe "Value Serialization" $ do

    let schema              = makeSchema [ ("a", DbtInt)
                                         , ("b", DbtString)
                                         , ("c", DbtString) ]
        record              = makeRecord 123 [ ("a", (DbInt 123))
                                             , ("b", (DbString "STRINGIE"))
                                             , ("c", (DbString "STRINGO"))]
        encoded@(key,value) = encodeRecord schema sid record
        indices             = decodeIndexes schema value

    it "reads out indexes from serialized items" $ do
      r <- decodeKey key
      r `shouldBe` 123

    it "reads out indexes from serialized items" $ do
      let decodeFn x = decodeFieldByIndex schema indices x value
      r <- decodeFn 0
      r `shouldBe` (DbInt 123)

    it "reads out indexes from serialized items" $
      indices `shouldBe` [4,8,7]

    it "reads out indexes from serialized items" $ do
      let decodeFn x = decodeFieldByIndex schema indices x value
      r0 <- decodeFn 0
      r0 `shouldBe` (DbInt    123)
      r1 <- decodeFn 1
      r1 `shouldBe` (DbString "STRINGIE")
      r2 <- decodeFn 2
      r2 `shouldBe` (DbString "STRINGO")

    it "decodes certain serialized values" $ do
      let decodeFn x = decodeRecord (Field x) schema encoded
      r <- decodeFn "a"
      r `shouldBe` (makeRecord 123 [ ("a", (DbInt 123))])

    it "decodes a complete serialized value" $ do
      let decodeFn _x = decodeRecord Record schema encoded
      r <- decodeFn ("a"::String)
      r `shouldBe` record

  -- describe "Partial Serialization" $ do
  --   -- OKAY HERES THE BUG
  --   it "decodes the record that was only partially encoded" $ do
  --     let schema = makeSchema [ ("a", DbtString)
  --                             , ("b", DbtLong)
  --                             , ("c", DbtLong) ]
  --         record  = makeRecord 123 [("a", (DbString "STRINGIE"))]
  --         encoded = encodeRecord schema record 1
  --         decodeFn = \_x -> decodeRecord Record schema encoded
  --     decodeFn ("a"::String) `shouldBe` (Right record)
