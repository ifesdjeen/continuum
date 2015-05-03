{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

{-# OPTIONS_GHC -fno-warn-orphans            #-}

module Continuum.Serialization.RecordSpec where

import           Data.ByteString       ( ByteString )
import           Data.ByteString.Char8 ( pack )
import Continuum.Serialization.Record
import Continuum.Types
import Continuum.Core

import Test.Hspec

import Test.QuickCheck

-- import Debug.Trace

testSchema :: DbSchema
testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)
                        , ("c", DbtString) ]


instance Arbitrary DbType where
  arbitrary = elements [ DbtLong, DbtInt, DbtByte, DbtShort, DbtFloat, DbtDouble, DbtString]

data SchemaTestRow = TestRow ByteString DbType DbValue
                   deriving (Show)

instance Arbitrary ByteString where
  arbitrary = pack <$> suchThat arbitrary (\str -> and [not $ any (\x -> '\NUL' == x) str,
                                                        not (length str == 0) ])
instance Arbitrary SchemaTestRow where
  arbitrary = do
    name <- arbitrary
    tp <- arbitrary
    val <- case tp of
            DbtLong   -> DbLong   <$> suchThat arbitrary (\i -> i > 0)
            DbtInt    -> DbInt    <$> suchThat arbitrary (\i -> i > 0)
            DbtByte   -> DbByte   <$> suchThat arbitrary (\i -> i > 0)
            DbtShort  -> DbShort  <$> suchThat arbitrary (\i -> i > 0)
            DbtFloat  -> DbFloat  <$> suchThat arbitrary (\i -> i > 0)
            DbtDouble -> DbDouble <$> suchThat arbitrary (\i -> i > 0)
            DbtString -> DbString <$> arbitrary
    return $ TestRow name tp val

roundTrip :: [SchemaTestRow] ->
             Bool
roundTrip testRows =
  let schema = makeSchema $ fmap (\(TestRow name tp _) -> (name, tp)) testRows
      record = makeRecord 123 $ fmap (\(TestRow name _ vl) -> (name, vl)) testRows
      encoded = encodeRecord schema record 1
      decoder = decodeRecord Record schema
  in
   (Right record) == (decoder encoded)


spec :: Spec
spec = do

  describe "QC" $ do
    it "passes Round Trip" $ do
      property $ roundTrip

  -- One more bug: negative numbers are decoded wrong
  describe "Serialization" $ do

    let testSchema2 = makeSchema [ ("a", DbtLong)]
        record      = makeRecord 123 [ ("a", (DbLong (-2))) ]
        encoded     = encodeRecord testSchema2 record 1
        indices     = decodeIndexes testSchema2 (snd encoded)

    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeFieldByIndex testSchema2 indices x (snd encoded)
      decodeFn 0 `shouldBe` (Right $ DbLong (-2))

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
      let decodeFn = \_x -> decodeRecord Record testSchema encoded
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
