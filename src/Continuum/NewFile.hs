module Continuum.NewFile
       where

import           Control.Monad.State.Strict
import           Control.Monad.Trans.Resource
import qualified Database.LevelDB.MonadResource  as Base
import           Data.Maybe (isJust, fromJust)
import           Data.ByteString        (ByteString)
import           Database.LevelDB.MonadResource (DB, WriteOptions, ReadOptions,
                                                 Iterator,
                                                 iterSeek, iterFirst, -- iterItems,
                                                 withIterator, iterNext, iterEntry)

scanIntern :: (MonadResource m) =>
               Iterator
               -> (acc -> (ByteString, ByteString) -> acc)
               -> acc
               -> m acc

scanIntern iter op orig = s
  where s = do
          next <- iterEntry iter
          iterNext iter

          if isJust next
            then do
            a <- s
            return $ (op a (fromJust next))

            else return orig



stateMonadScanIntern :: (MonadResource m) =>
               Iterator
               -> (acc -> (ByteString, ByteString) -> acc)
               -> acc
               -> m acc

stateMonadScanIntern iter op orig = execStateT s orig
  where s = do
          next <- iterEntry iter
          iterNext iter

          if isJust next
            then do
            a <- get
            let b = op a (fromJust next)
            put $ seq b b
            s
            else return ()
