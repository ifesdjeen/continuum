{-# LANGUAGE FlexibleContexts #-}

module Continuum.Tryout where

import Database.LevelDB
-- import Control.Monad.Reader
import Control.Monad.State
import Continuum.Storage
import Continuum.Serialization

-- dbOps :: ReaderT DBContext IO ()
-- dbOps = do
--   putDbValue (DbString "asd") (DbString "bsd")
--   --- liftIO $ putStrLn ("Hello")
--   return ()

main :: IO ()
main = do
  runResourceT $ do
    db <- open "/tmp/leveltest"
               defaultOptions{ createIfMissing = True
                             , cacheSize= 2048
                             }
    let ctx = makeContext db (makeSchema [("a", DbtInt), ("b", DbtString)]) (defaultReadOptions, defaultWriteOptions)
        -- a = (runReader dbOps) ctx

    liftIO $ runStateT (putDbValue (DbString "asd") (DbString "bsd")) ctx
             >> runStateT (putDbValue (DbString "bdd") (DbString "dsd")) ctx
             >> evalStateT (getDbValue (DbString "bdd")) ctx >>= print

    return ()
