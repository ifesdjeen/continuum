{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Serialization.Test where

import Continuum.Serialization

import Test.Hspec
import Test.Hspec.Expectations
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

import Control.Monad (liftM, void)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Control.Monad.Reader

testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)
                        , ("c", DbtString) ]
main :: IO ()
main =  hspec $ do

  describe "Basic DB Functionality" $ do

    it "should put items into the database and retrieve them" $  do
      let encoded = indexingEncodeRecord testSchema record 1
          indices = decodeIndexes testSchema (snd encoded)
          record  = makeRecord 123 [("a", (DbInt 123))
                                   , ("b", (DbString "STRINGIE"))
                                   , ("c", (DbString "STRINGO"))]

      (return $ indices) `shouldReturn` (Right [6,17,16])
