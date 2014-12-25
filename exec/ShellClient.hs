{-# LANGUAGE OverloadedStrings #-}

module Main where


import qualified Data.Map               as Map

import           Data.Aeson
import           Data.ByteString.Lazy.Char8  ( unpack )
import           Data.Text                   ( pack )
import           Data.Text.Encoding          ( decodeUtf8 )
import           Continuum.Client.Base       ( DbRecord(..), DbResult(..), SelectQuery(..), Request(..), DbValue(..), connect, sendRequest )

-- instance Continuum.DbValue ToJSON where

main :: IO ()
main = return ()
