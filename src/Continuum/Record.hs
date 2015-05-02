module Continuum.Record where

import           Data.ByteString                   ( ByteString )
import qualified Data.Map                       as Map

import Continuum.Types

-- | Creates a DbRecord from Timestamp and Key/Value pairs
--
makeRecord :: Integer -> [(ByteString, DbValue)] -> DbRecord
makeRecord timestamp vals = DbRecord timestamp (Map.fromList vals)

getValue :: FieldName -> DbRecord -> Maybe DbValue
getValue fieldName (DbRecord _ recordFields) = Map.lookup fieldName recordFields
