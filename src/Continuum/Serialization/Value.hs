module Continuum.Serialization.Value
       (
         encodeValue
       , decodeValue
       ) where

import Data.ByteString        ( ByteString )
import Continuum.Types        ( DbValue(..),
                                DbType(..),
                                DbErrorMonad )
import Control.Monad.Catch  ( MonadMask(..), throwM )
import Continuum.Serialization.Primitive


encodeValue :: DbValue -> ByteString
encodeValue (DbLong   value) = packWord64 value
encodeValue (DbInt    value) = packWord32 value
encodeValue (DbShort  value) = packWord16 value
encodeValue (DbByte   value) = packWord8  value
encodeValue (DbFloat  value) = packFloat value
encodeValue (DbDouble value) = packDouble value
encodeValue (DbString value) = value

decodeValue :: (MonadMask m) => DbType -> ByteString -> m DbValue
decodeValue DbtLong    bs  = DbLong   <$> (unpackWord64 bs)
decodeValue DbtInt     bs  = DbInt    <$> (unpackWord32 bs)
decodeValue DbtShort   bs  = DbShort  <$> (unpackWord16 bs)
decodeValue DbtByte    bs  = DbByte   <$> (unpackWord8 bs)
decodeValue DbtFloat   bs  = DbFloat  <$> (unpackFloat bs)
decodeValue DbtDouble  bs  = DbDouble <$> (unpackDouble bs)
decodeValue DbtString  bs  = return $ DbString bs
