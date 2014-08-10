module Continuum.Options where

import Database.LevelDB

opts :: Options
opts = defaultOptions{ createIfMissing = True
                        , cacheSize= 2048
                                     -- , comparator = Just customComparator
                        }

readOpts = defaultReadOptions
writeOpts = defaultWriteOptions
