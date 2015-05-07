{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

{-# OPTIONS_GHC -fno-warn-orphans            #-}

module Continuum.Serialization.RecordSpec where

import Continuum.Serialization.Record
import Continuum.Serialization.Schema ( makeSchema )
import Continuum.Types

import Continuum.Support.QuickCheck

import Test.Hspec
import Test.QuickCheck

sid :: Integer
sid = 456

roundTrip :: [SchemaTestRow] -> Bool
roundTrip testRows =
  let schema  = makeSchema $ fmap (\(TestRow name tp _) -> (name, tp)) testRows
      record  = makeRecord 123 $ fmap (\(TestRow name _ vl) -> (name, vl)) testRows
      encoded = encodeRecord schema sid record
      decoder = decodeRecord Record schema
  in
   (Right record) == (decoder encoded)

spec :: Spec
spec = do

  describe "Property Test" $ do
    it "passes Round Trip" $ do
      property $ roundTrip

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
      decodeKey key `shouldBe` (Right $ 123)

    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeFieldByIndex schema indices x value
      decodeFn 0 `shouldBe` (Right $ DbInt 123)

    it "reads out indexes from serialized items" $
      indices `shouldBe` [4,8,7]

    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeFieldByIndex schema indices x value
      decodeFn 0 `shouldBe` (Right $ DbInt    123)
      decodeFn 1 `shouldBe` (Right $ DbString "STRINGIE")
      decodeFn 2 `shouldBe` (Right $ DbString "STRINGO")

    it "decodes certain serialized values" $ do
      let decodeFn = \x -> decodeRecord (Field x) schema encoded
      decodeFn "a" `shouldBe` (Right $ makeRecord 123 [ ("a", (DbInt 123))])

    it "decodes a complete serialized value" $ do
      let decodeFn = \_x -> decodeRecord Record schema encoded
      decodeFn ("a"::String) `shouldBe` (Right record)

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
