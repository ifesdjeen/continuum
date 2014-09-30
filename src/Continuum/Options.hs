module Continuum.Options where

import           Database.LevelDB


opts :: Options
opts = defaultOptions{ createIfMissing = True
                     , cacheSize= 512 * 1048576
                     -- , blockSize= 2048
                     -- , compression = NoCompression
                     -- , compression = NoCompressionNoCompression
                     -- , comparator = Just customComparator
                        }

readOpts :: ReadOptions
readOpts = defaultReadOptions

writeOpts :: WriteOptions
writeOpts = defaultWriteOptions
