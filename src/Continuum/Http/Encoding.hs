{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE OverloadedStrings #-}

module Continuum.Http.Encoding where

import qualified Data.Map               as Map

import           Data.Aeson
import           Data.ByteString             ( ByteString )
import           Data.ByteString.Lazy.Char8  ( unpack )
import           Data.Text                   ( pack )
import           Data.Text.Encoding          ( decodeUtf8 )
import           Continuum.Common.Types

instance ToJSON DbError where
  toJSON (NotEnoughInput should was) = toJSON $ "Not Enough Input: " ++ (show should) ++ ", but was: " ++ (show was)
  toJSON err = toJSON $ show err

instance ToJSON ByteString where
  toJSON = toJSON . decodeUtf8

instance (ToJSON a, ToJSON b) => ToJSON (Map.Map a b) where
  toJSON m = object $
             concat $
             map (\(k, v) -> [(pack $ show $ toJSON k) .= (toJSON v)]) (Map.toList m)

instance ToJSON DbSchema where
  toJSON DbSchema{..} = toJSON $ schemaMappings

instance ToJSON DbType

instance ToJSON DbValue where
  toJSON (DbInt v)    = toJSON v
  toJSON (DbLong v)   = toJSON v
  toJSON (DbShort v)  = toJSON v
  toJSON (DbString v) = toJSON $ decodeUtf8 v
  toJSON (DbFloat v)  = toJSON v
  toJSON (DbDouble v) = toJSON v

instance ToJSON DbResult where
  toJSON EmptyRes         = toJSON ("" :: String)
  toJSON (ValueRes  r)    = toJSON r
  toJSON (ListResult r)   = toJSON r
  toJSON (MapResult vals) = object $
                            concat $
                            map (\(k, v) -> [(pack (show k)) .= v]) (Map.toList vals)
  toJSON (RecordRes
          (DbRecord ts vals)) = object $
                                concat $
                                ["timestamp" .= ts] :
                                map (\(k, v) -> [(decodeUtf8 k) .= v]) (Map.toList vals)
  toJSON a = toJSON $ show a
