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

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()



main :: IO ()
main = runApp testDBPath $ do
  liftIO $ cleanup
  putRecord (DbRecord
             123
             (Map.fromList [("a", (DbInt 1)), ("b", (DbString "1"))]))

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







newtype ReaderIO s a = ReaderIO { runReaderIO :: s -> IO a }

-- getLine :: IO String

myGetLine :: ReaderIO s String
myGetLine = ReaderIO $ (\x -> getLine)

liftReaderIO :: IO a -> ReaderIO s a
liftReaderIO a = ReaderIO $ (\x -> a)

myGetLine' :: ReaderIO s String
myGetLine' = liftReaderIO $ getLine

-- newtype MyReader m s a = MyReader { runMyReader :: s -> m a }

newtype MyReaderT s m a = MyReaderT { runMyReaderT :: s -> m a }

liftMyReaderT :: (Monad m) => m a -> MyReaderT s m a
liftMyReaderT a = MyReaderT $ (\x -> a)


newtype MyCombination s a = MyCombination { runMyCombination :: s -> IO a }
