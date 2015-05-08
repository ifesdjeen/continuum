{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

{-# OPTIONS_GHC -fno-warn-orphans            #-}

module Continuum.Support.QuickCheck where

import Data.ByteString       ( ByteString )
import Data.ByteString.Char8 ( pack )
import Continuum.Types
import Test.QuickCheck
import Continuum.Serialization.Schema ( makeSchema, validate )
import Continuum.Serialization.Record ( makeRecord )
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

instance Arbitrary DbValue where
  arbitrary = do
    oneof [DbLong    <$> arbitrary
          , DbInt    <$> arbitrary
          , DbByte   <$> arbitrary
          , DbShort  <$> arbitrary
          , DbFloat  <$> arbitrary
          , DbDouble <$> arbitrary
          , DbString <$> arbitrary]


instance Arbitrary DbSchema where
  arbitrary = do
    defs <- suchThat arbitrary (not . null)
    return $ makeSchema defs

instance Arbitrary DbRecord where
  arbitrary = do
    timestamp <- arbitrary
    vals <- suchThat arbitrary (not . null)
    return $ makeRecord timestamp vals

instance CoArbitrary DbSchema where
  coarbitrary schema = undefined

instance Arbitrary (DbSchema, [DbRecord]) where
  arbitrary = do
    schema  <- arbitrary
    records <- suchThat (listOf $ suchThat arbitrary (validate schema)) (not . null)
    return (schema, records)
