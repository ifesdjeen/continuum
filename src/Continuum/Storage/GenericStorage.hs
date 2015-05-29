module Continuum.Storage.GenericStorage ( keySlice
                                        , entrySlice

                                          -- LevelDB.Base
                                        , withIter
                                        , write
                                        , open
                                        , defaultOptions
                                        , createIfMissing
                                        , destroy

                                        , def) where

import Control.Monad.IO.Class
import Continuum.Serialization.Record ( decodeRecord, getValue )
import Database.LevelDB.Streaming     ( keySlice, entrySlice )
import Database.LevelDB.Base          ( withIter, write, open, defaultOptions, createIfMissing, destroy )
import Data.Default                   ( def )
import qualified Database.LevelDB.Streaming as S
import Continuum.Types
