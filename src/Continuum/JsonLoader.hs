{-# LANGUAGE OverloadedStrings #-}

module Continuum.JsonLoader where

import Control.Applicative ((<$>), (<*>), empty)
import Data.Aeson
import Data.Maybe
import Control.Monad
import qualified Data.ByteString.Lazy.Char8 as BL
import Control.Monad.IO.Class

import qualified Data.Set as Set
import Continuum.Storage
import Continuum.Serialization
import Continuum.Actions

data Entry = Entry { request_ip :: String
                   , status :: String
                   , host :: String
                   , uri :: String
                   , date :: Integer }
             deriving (Show)


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
testDBPath = "/tmp/haskell-leveldb-tests"


main :: IO ()
main = do
  content <- readFile "/Users/ifesdjeen/hackage/continuum/data.json"
  line <- return $ lines content
  decoded <- return $ (map decodeStr line)

  -- putStrLn $ show decoded

  runApp testDBPath $ do
    forM_ decoded $ \x ->

      putRecord (makeRecord (date x)
                          [("request_ip", DbString (request_ip x)),
                           ("host",       DbString (host x)),
                           ("uri",        DbString (uri x)),
                           ("status",     DbString (status x))
                          ])

  return ()
  -- print decoded
  -- putStrLn $ show linesOfFile

    -- let req = decode "{\"x\":3.0,\"y\":-1.0}" :: Maybe Coord
    -- print req
    -- let reply = Coord 123.4 20
    -- BL.putStrLn (encode reply)

showAll = runApp testDBPath $ do
  records <- scanAll id
               (:)
               []
               -- Set.empty

  -- liftIO $ putStrLn "===== ALL ===== "
  -- liftIO $ putStrLn (show $ take 5 records)

  let count _ acc = acc + 1
      a = foldGroup count 0 $ groupBy records (byFieldMaybe "status")
  liftIO $ putStrLn (show $ a)
  return ()

  -- liftIO $ putStrLn (show $ foldl Set.insert Set.empty (extractField "status") <$> c)
