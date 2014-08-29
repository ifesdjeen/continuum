{-# LANGUAGE DeriveGeneric #-}

module Continuum.Types where

import           Data.ByteString        (ByteString)
import           GHC.Generics           (Generic)

data DbError = IndexesDecodeError String
             | FieldDecodeError String ByteString
             | ValuesDecodeError String
             | ValueDecodeError String
             | KeyDecodeError String
             | FieldNotFoundError
             | DecodeFieldByIndexError String [Int]
             | OtherError
             deriving (Show, Eq, Ord, Generic)

type AggregationFn acc = ((ByteString, ByteString) -> acc -> (Either DbError acc))

-- type AggregationPipeline acc = ((ByteString, ByteString) -> acc -> (Either String acc))
