{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import qualified Data.Map as Map
import           Data.ByteString.Char8 (pack)
import Data.Time.Clock.POSIX
import System.Process(system)
import           Data.List (elemIndex)
import Control.Applicative ((<$>), (<*>), empty)
import Data.Aeson
import Data.Maybe
import Control.Monad
import qualified Data.ByteString.Lazy.Char8 as BL
import Control.Monad.IO.Class

import Continuum.Storage
import Continuum.Serialization
import Continuum.Types
import Continuum.Folds
import Continuum.Aggregation
import qualified Control.Foldl as L


data Entry = Entry { request_ip :: String
                   , status     :: String
                   , host       :: String
                   , uri        :: String
                   , date       :: Integer }
           deriving (Show)

prodSchema = makeSchema [ ("request_ip", DbtString)
                        , ("host",       DbtString)
                        , ("uri",        DbtString)
                        , ("status",     DbtString)]

instance FromJSON Entry where
    parseJSON (Object v) = Entry <$>
                             v .: "request-ip" <*>
                             v .: "status"     <*>
                             v .: "host"       <*>
                             v .: "uri"        <*>
                             v .: "date"
    parseJSON _          = empty

decodeStr :: String -> Entry
decodeStr = (fromJust . decode . BL.pack)

testDBPath :: String
testDBPath = "/tmp/production-data"


main2 :: IO ()
main2 = do

  liftIO $ cleanup

  content <- readFile "/Users/ifesdjeen/hackage/continuum/data.json"
  line <- return $ lines content
  decoded <- return $ (map decodeStr line)

  -- putStrLn $ show decoded

  runApp testDBPath prodSchema $ do
    forM_ decoded $ \x ->

      putRecord (makeRecord (date x)
                 [("request_ip", DbString (pack (request_ip x))),
                  ("host",       DbString (pack (host x))),
                  ("uri",        DbString (pack (uri x))),
                  ("status",     DbString (pack (status x)))
                 ])

  return ()

main = runApp testDBPath prodSchema $ do
  forM_ [0..100] $ \x -> do
          before <- liftIO $ getPOSIXTime
          a <- aggregateAllByField "status" (groupFold (\ (DbFieldResult (_, x)) -> (x, 0)) countFold)
          -- a <- aggregateAllByField "status" countFold
          after <- liftIO $ getPOSIXTime
          liftIO $ putStrLn $ show a
          liftIO $ putStrLn $ show (after - before)

  return ()

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()
