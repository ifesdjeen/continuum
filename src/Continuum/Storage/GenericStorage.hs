module Continuum.Storage.GenericStorage ( keySlice
                                        , entrySlice
                                        , withDecoded ) where

import Control.Monad.IO.Class
import Continuum.Serialization.Record ( decodeRecord )
import Database.LevelDB.Streaming ( keySlice, entrySlice )
import Database.LevelDB.Streaming as S
import Continuum.Types

withDecoded :: (Applicative m, MonadIO m) => Decoding -> DbSchema -> Stream m Entry -> Stream m (DbErrorMonad DbRecord)
withDecoded decoding schema = S.map (decodeRecord decoding schema)
