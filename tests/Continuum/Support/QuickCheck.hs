{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

{-# OPTIONS_GHC -fno-warn-orphans            #-}

module Continuum.Support.QuickCheck where

import Continuum.Types
import Data.List                      ( nubBy )
import Data.ByteString                ( ByteString )
import Data.ByteString.Char8          ( pack )
import Continuum.Serialization.Schema ( makeSchema )
import Continuum.Serialization.Record ( makeRecord )

import Test.QuickCheck
-- import Test.QuickCheck.Gen ( Gen(..) )

instance Arbitrary DbType where
  arbitrary = elements [ DbtLong
                       , DbtInt
                       , DbtByte
                       , DbtShort
                       , DbtFloat
                       , DbtDouble
                       , DbtString]

instance Arbitrary ByteString where
  arbitrary = pack <$> suchThat arbitrary notEmpty
    where notEmpty str = and [not $ any (\x -> '\NUL' == x) str,
                              not (length str == 0) ]

instance Arbitrary DbSchema where
  arbitrary = do
    defs <- suchThat arbitrary (not . null)
    return $ makeSchema defs

recordsBySchema :: DbSchema -> Gen [DbRecord]
recordsBySchema schema = fmap (nubBy ts) <$> listOf $ recordBySchema schema
  where ts (DbRecord ts1 _) (DbRecord ts2 _) = ts1 == ts2

recordBySchema :: DbSchema -> Gen DbRecord
recordBySchema schema = do
  i    <- suchThat arbitrary (\i -> i > 0)
  vals <- traverse valueGenerator (schemaTypes schema)  -- fmap valueGenerator (schemaTypes schema )
  return $ makeRecord i (zip (fields schema) vals)

valueGenerator :: DbType -> Gen DbValue
valueGenerator DbtLong   = DbLong   <$> suchThat arbitrary (\i -> i > 0)
valueGenerator DbtInt    = DbInt    <$> suchThat arbitrary (\i -> i > 0)
valueGenerator DbtByte   = DbByte   <$> suchThat arbitrary (\i -> i > 0)
valueGenerator DbtShort  = DbShort  <$> suchThat arbitrary (\i -> i > 0)
valueGenerator DbtFloat  = DbFloat  <$> suchThat arbitrary (\i -> i > 0)
valueGenerator DbtDouble = DbDouble <$> suchThat arbitrary (\i -> i > 0)
valueGenerator DbtString = DbString <$> arbitrary

instance Arbitrary (DbSchema, [DbRecord]) where
  arbitrary = do
    schema  <- arbitrary
    records <- recordsBySchema schema
    return (schema, records)

instance Arbitrary (DbSchema, DbRecord) where
  arbitrary = do
    schema <- arbitrary
    record <- recordBySchema schema
    return (schema, record)
