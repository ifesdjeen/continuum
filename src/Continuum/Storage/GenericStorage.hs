module Continuum.Storage.GenericStorage ( keySlice
                                        , entrySlice
                                        , withDecoded

                                          -- LevelDB.Base
                                        , withIter
                                        , write
                                        , open
                                        , defaultOptions
                                        , createIfMissing
                                        , destroy

                                        , def) where

import Control.Monad.IO.Class
import Continuum.Serialization.Record ( decodeRecord )
import Database.LevelDB.Streaming     ( keySlice, entrySlice )
import Database.LevelDB.Base          ( withIter, write, open, defaultOptions, createIfMissing, destroy )
import Data.Default                   ( def )
import qualified Database.LevelDB.Streaming as S
import Continuum.Types

withDecoded :: (Applicative m, MonadIO m) => Decoding -> DbSchema -> Stream m Entry -> Stream m (DbErrorMonad DbRecord)
withDecoded decoding schema = S.map (decodeRecord decoding schema)
