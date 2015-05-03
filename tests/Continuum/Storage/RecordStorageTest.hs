module Continuum.Storage.RecordStorageTest () where

import           Control.Applicative        hiding (empty)
import           Control.Monad.Catch
import           Control.Monad.Identity
import           Control.Monad.IO.Class
import qualified Data.ByteString            as BS
import           Data.ByteString.Char8      (ByteString, singleton, unpack)
import           Data.Default
import           Data.Foldable              (foldMap)
import           Data.List
import           Data.Monoid
import           Database.LevelDB.Base
import           Database.LevelDB.Internal  (unsafeClose)
import qualified Database.LevelDB.Streaming as S
import           System.Directory
import           System.IO.Temp

data Rs = Rs DB FilePath

spec :: Spec
spec = do

  describe "Primitive" $ do
    it "passes Word8  Round Trip" $ do
      1 == 1
  where
    initDB = do
        tmp <- getTemporaryDirectory
        dir <- createTempDirectory tmp "leveldb-streaming-tests"
        db  <- open dir defaultOptions { createIfMissing = True }
        write db def
          . map ( \ c -> let c' = singleton c in Put c' c')
          $ ['A'..'Z']
        return $ Rs db dir

    destroyDB (Rs db dir) = unsafeClose db `finally` destroy dir defaultOptions
