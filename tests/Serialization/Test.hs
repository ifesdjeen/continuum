{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Serialization.Test where

import Continuum.Serialization
import Continuum.Types

import Test.Hspec
import Test.Hspec.Expectations

import Control.Monad (liftM, void)
import Control.Monad.IO.Class (MonadIO (liftIO))

import Control.Monad.Reader

testSchema = makeSchema [ ("a", DbtInt)
                        , ("b", DbtString)
                        , ("c", DbtString) ]
main :: IO ()
main =  hspec $ do

  describe "Serialization" $ do

    let full    = indexingEncodeRecord testSchema record 1
        encoded = snd $ full
        record  = makeRecord 123 [ ("a", (DbInt 123))
                                 , ("b", (DbString "STRINGIE"))
                                 , ("c", (DbString "STRINGO"))]
        indices = decodeIndexes testSchema encoded
    it "reads out indexes from serialized items" $
      indices `shouldBe` (Right [6,17,16])

    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeFieldByIndex indices x encoded
      decodeFn 0 `shouldBe` (Right $ DbInt    123)
      decodeFn 1 `shouldBe` (Right $ DbString "STRINGIE")
      decodeFn 2 `shouldBe` (Right $ DbString "STRINGO")


    it "reads out indexes from serialized items" $ do
      let decodeFn = \x -> decodeFieldByName x testSchema full
      decodeFn "a" `shouldBe` (Right $ DbInt    123)
      decodeFn "b" `shouldBe` (Right $ DbString "STRINGIE")
      decodeFn "c" `shouldBe` (Right $ DbString "STRINGO")
