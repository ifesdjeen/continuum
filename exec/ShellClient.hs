{-# LANGUAGE OverloadedStrings #-}

module Main where


import qualified Data.Map               as Map

import           Data.Aeson
import           Data.Text.Encoding     ( decodeUtf8 )
import           Continuum.Client.Base  ( DbRecord(..), DbResult(..), SelectQuery(..), Request(..), DbValue(..), connect, sendRequest )

-- instance Continuum.DbValue ToJSON where

instance ToJSON DbValue where
  toJSON (DbInt i) = toJSON i
  toJSON (DbString i) = toJSON (decodeUtf8 i)

instance ToJSON DbResult where
  toJSON EmptyRes = toJSON ("" :: String)
  toJSON (DbResults r) = toJSON r
  toJSON (RecordRes
          (DbRecord ts vals)) = object $
                                concat $
                                ["timestamp" .= ts] :
                                map (\(k, v) -> [(decodeUtf8 k) .= v]) (Map.toList vals)

main :: IO ()
main = do
  client <- connect "127.0.0.1" "5566"
  res    <- sendRequest client (Select "memory" FetchAll)

  case res of
    (Right r)  -> print $ encode $ toJSON r
    (Left err) -> print $ "error:" ++ (show err)

  return ()
