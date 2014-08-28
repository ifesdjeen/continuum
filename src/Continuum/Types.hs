module Continuum.Types where

import Data.ByteString        (ByteString)


type AggregationFn acc = ((ByteString, ByteString) -> acc -> (Either String acc))

-- type AggregationPipeline acc = ((ByteString, ByteString) -> acc -> (Either String acc))
