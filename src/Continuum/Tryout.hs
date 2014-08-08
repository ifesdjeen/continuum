{-# LANGUAGE FlexibleContexts #-}

module Continuum.Tryout where

import System.Process(system)

import Database.LevelDB
-- import Control.Monad.Reader
import Control.Monad.State
import Continuum.Storage
import Continuum.Serialization

import qualified Data.Map as Map

-- dbOps :: ReaderT DBContext IO ()
-- dbOps = do
--   putDbValue (DbString "asd") (DbString "bsd")
--   --- liftIO $ putStrLn ("Hello")
--   return ()

testDBPath :: String
testDBPath = "/tmp/haskell-leveldb-tests"

customComparator :: Comparator
customComparator = Comparator compareKeys

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()

main :: IO ()
main = do
  liftIO $ cleanup
  runResourceT $ do
    db <- open testDBPath
               defaultOptions{ createIfMissing = True
                             , cacheSize= 2048
                             -- , comparator = Just customComparator
                             }
    let ctx = makeContext db (makeSchema [("a", DbtInt), ("b", DbtString)]) (defaultReadOptions, defaultWriteOptions)
        -- a = (runReader dbOps) ctx

    liftIO $ (flip runStateT) ctx $ do

      putRecord (DbRecord
                 123
                 (Map.fromList [("a", (DbInt 1)), ("b", (DbString "1"))]))
      putRecord (DbRecord
                 123
                 (Map.fromList [("a", (DbInt 2)), ("b", (DbString "2"))]))
      putRecord (DbRecord
                 123
                 (Map.fromList [("a", (DbInt 3)), ("b", (DbString "3"))]))

      putRecord (DbRecord
                 789
                 (Map.fromList [("a", (DbInt 4)), ("b", (DbString "4"))]))

      -- a <- getDbValue2 ((123), (Just 1))
      -- liftIO $ putStrLn (show a)

      -- a <- getDbValue2 123
      -- liftIO $ putStrLn (show a)

      a <- findTs 123
      liftIO $ putStrLn "===== 123 ===== "
      liftIO $ putStrLn (show a)

      range <- findRange 123 789
      liftIO $ putStrLn "===== RANGE ===== "
      liftIO $ putStrLn (show range)
      liftIO $ putStrLn "===== RANGE ===== "

      c <- findAll
      liftIO $ putStrLn "===== ALL ===== "
      liftIO $ putStrLn (show c)


      return ()

      -- >> evalStateT (getDbValue (DbString "bdd")) ctx >>= print

    -- liftIO $ runStateT (putDbValue (DbString "asd") (DbString "bsd")) ctx
    --          >> runStateT (putRecord (DbRecord
    --                                   123
    --                                   Nothing
    --                                   (Map.fromList [("a", (DbInt 1)), ("b", (DbString "asdyoyoyoyo"))]))) ctx
    --          >> runStateT (putRecord (DbRecord
    --                                   123
    --                                   Nothing
    --                                   (Map.fromList [("a", (DbInt 1)), ("b", (DbString "anooother"))]))) ctx
    --          >> runStateT (putRecord (DbRecord
    --                                   123
    --                                   Nothing
    --                                   (Map.fromList [("a", (DbInt 1)), ("b", (DbString "anooother"))]))) ctx
    --          >> runStateT (putDbValue (DbString "bdd") (DbString "dsd")) ctx
    --          -- >> evalStateT (getDbValue (DbString "bdd")) ctx >>= print
    --          >> evalStateT (getDbValue2 ((123), (Just 1))) ctx >>= print
    return ()
