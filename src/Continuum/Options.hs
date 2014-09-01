module Continuum.Options where

import Database.LevelDB

opts :: Options
opts = defaultOptions{ createIfMissing = True
                     , cacheSize= 83886080
                     -- , blockSize= 2048
                     -- , compression = NoCompression
                     -- , compression = NoCompressionNoCompression
                     -- , comparator = Just customComparator
                        }

readOpts = defaultReadOptions
writeOpts = defaultWriteOptions
