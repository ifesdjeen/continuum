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
import           Continuum.Common.Types
import           Continuum.Http.Encoding
import           Continuum.Storage.Parallel     ( parallelScan )

import           Network.Wai.Middleware.Static  ( static, staticPolicy, addBase )

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
      db   <- Scotty.param "db"
      from <- maybeParam   "from"
      to   <- maybeParam   "to"
      res  <- liftIO $ processRequest (parallelScan db (toRange from to) Record FetchAll)
      Scotty.json res

  return ()
  where

maybeParam :: Scotty.Parsable a => Text -> Scotty.ActionM (Maybe a)
maybeParam p = (Just <$> Scotty.param p) `Scotty.rescue` (\_ -> (return Nothing))

toRange :: Maybe Integer -> Maybe Integer -> ScanRange
toRange (Just from) (Just to) = KeyRange from to
toRange (Just from) Nothing   = OpenEnd from
toRange Nothing Nothing       = EntireKeyspace
