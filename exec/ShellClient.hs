{-# LANGUAGE OverloadedStrings #-}

module Main where


import qualified Data.Map               as Map

import           Data.Aeson
import           Data.ByteString.Lazy.Char8  ( unpack )
import           Data.Text                   ( pack )
import           Data.Text.Encoding          ( decodeUtf8 )
import           Continuum.Client.Base       ( DbRecord(..), DbResult(..), SelectQuery(..), Request(..), DbValue(..), connect, sendRequest )

-- instance Continuum.DbValue ToJSON where

instance ToJSON DbValue where
  toJSON (DbInt i) = toJSON i
  toJSON (DbString i) = toJSON (decodeUtf8 i)

instance ToJSON DbResult where
  toJSON EmptyRes = toJSON ("" :: String)
  toJSON (CountStep r) = toJSON r
  toJSON (DbResults r) = toJSON r
  toJSON (GroupRes vals) = object $
                        concat $
                        map (\(k, v) -> [(pack (show k)) .= v]) (Map.toList vals)
  toJSON (RecordRes
          (DbRecord ts vals)) = object $
                                concat $
                                ["timestamp" .= ts] :
                                map (\(k, v) -> [(decodeUtf8 k) .= v]) (Map.toList vals)

main :: IO ()
main = do
  client <- connect "127.0.0.1" "5566"
  -- res    <- sendRequest client (Select "memory" FetchAll)
  res    <- sendRequest client (Select "memory" (Group "subtype" Count))

  case res of
    (Right r)  -> putStrLn $ unpack $ encode $ toJSON r
    (Left err) -> print $ "error:" ++ (show err)

  return ()
