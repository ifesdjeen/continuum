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
import           Continuum.Types

instance ToJSON (Map.Map ByteString (DbSchema, a)) where
  toJSON dbsMap = object $
                  concat $
                  map (\(k, v) -> [(decodeUtf8 k) .= (fst v)]) (Map.toList dbsMap)

instance ToJSON DbSchema where
  toJSON DbSchema{..} = object $
                        concat $
                        map (\(k, v) -> [(decodeUtf8 k) .= (show v)]) (Map.toList schemaMappings)


instance ToJSON DbValue where
  toJSON (DbInt i) = toJSON i
  toJSON (DbString i) = toJSON (decodeUtf8 i)

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
