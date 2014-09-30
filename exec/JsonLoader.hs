{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (loadData, processData, main) where

import           Data.ByteString.Char8 (pack)
import           Continuum.ParallelStorage
import           Data.Time.Clock.POSIX
import           System.Process (system)
import           Control.Applicative ((<$>), (<*>), empty)
import           Data.Aeson
import           Data.Maybe
import           Control.Monad
import           Control.Monad.IO.Class

import qualified Data.ByteString.Lazy.Char8 as BL

import           Continuum.Storage
import           Continuum.Types



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


loadData :: IO (DbErrorMonad DbResult)
loadData = do
  -- liftIO $ cleanup

  content <- readFile "/Users/ifesdjeen/hackage/continuum/data.json"
  line    <- return $ lines content
  decoded <- return $ (map decodeStr line)

  -- putStrLn $ show decoded

  runApp testDBPath $ do
    createDatabase "testdb" prodSchema

    forM_ decoded $ \x ->

      putRecord "testdb" (makeRecord (date x)
                 [("request_ip", DbString (pack (request_ip x))),
                  ("host",       DbString (pack (host x))),
                  ("uri",        DbString (pack (uri x))),
                  ("status",     DbString (pack (status x)))
                 ])
    return $ Right EmptyRes

processData :: IO (DbErrorMonad DbResult)
processData = runApp testDBPath $ do
  before <- liftIO $ getPOSIXTime
  a <- parallelScan "testdb"
  after <- liftIO $ getPOSIXTime
  liftIO $ putStrLn $ show a
  liftIO $ putStrLn $ show (after - before)

  return $ Right EmptyRes

cleanup :: IO ()
cleanup = system ("rm -fr " ++ testDBPath) >> return ()

main :: IO (DbErrorMonad DbResult)
main = processData
