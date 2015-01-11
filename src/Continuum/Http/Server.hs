{-# LANGUAGE OverloadedStrings #-}

module Continuum.Http.Server where

import qualified Data.Map                       as Map
import           Data.Text.Lazy                 ( Text )
import           Control.Concurrent.STM
import           Control.Concurrent             ( forkIO )
import           Control.Monad.State.Strict     ( evalStateT )
import qualified Web.Scotty as Scotty
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Applicative            ( (<$>) )
import           Continuum.Context
import           Continuum.Types
import           Continuum.Http.Encoding
import           Continuum.Storage.Parallel     ( parallelScan )

import           Network.Wai.Middleware.Static  ( static, staticPolicy, addBase )
import           Data.ByteString                ( ByteString )
import           Data.ByteString.Char8          ( split )

runWebServer :: (TVar DbContext) -> IO ()
runWebServer ctxTVar = do
  let processRequest op = evalStateT op ctxTVar
  _ <- forkIO $ Scotty.scotty 3000 $ do

    Scotty.middleware (staticPolicy (addBase "/Users/ifesdjeen/hackage/continuum/public"))

    Scotty.get "/" $ do
      res <- processRequest getSequenceNumber
      Scotty.json res

    Scotty.get "/dbs" $ do
      res <- processRequest getCtxDbs
      Scotty.json (Map.map fst res)

    Scotty.get "/dbs/:db/range" $ do
      db        <- Scotty.param "db"
      from      <- maybeParam   "from"
      to        <- maybeParam   "to"

      timeGroup <- maybeParam   "timeGroup"
      aggregate <- maybeParam   "aggregate"
      fields    <- maybeParam   "fields"

      let q = toQuery timeGroup aggregate fields

      res       <- liftIO $ processRequest (parallelScan db (toRange from to) Record q)
      Scotty.json res

  return ()
  where

toQuery :: Maybe Integer -> Maybe ByteString -> Maybe ByteString -> SelectQuery
toQuery (Just timeGroup) (Just aggregate) (Just fieldsStr) =
  let fields = splitFields fieldsStr
  in TimeGroup (Milliseconds timeGroup) (Multi (map (\field -> (field, Min field)) fields))
  where splitFields a = split ',' a
toQuery _ _ _ = FetchAll




maybeParam :: Scotty.Parsable a => Text -> Scotty.ActionM (Maybe a)
maybeParam p = (Just <$> Scotty.param p) `Scotty.rescue` (\_ -> (return Nothing))

toRange :: Maybe Integer -> Maybe Integer -> ScanRange
toRange (Just from) (Just to) = KeyRange from to
toRange (Just from) Nothing   = OpenEnd from
toRange Nothing Nothing       = EntireKeyspace
