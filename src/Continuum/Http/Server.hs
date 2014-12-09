{-# LANGUAGE OverloadedStrings #-}

module Continuum.Http.Server where

import           Control.Concurrent.STM
import           Control.Concurrent             ( forkIO )
import           Control.Monad.State.Strict     ( evalStateT )
import qualified Web.Scotty as Scotty


import           Continuum.Types
import           Continuum.Http.Encoding

runWebServer :: (TVar DbContext) -> IO ()
runWebServer ctxTVar = do
  let processRequest op = evalStateT op ctxTVar
  _ <- forkIO $ Scotty.scotty 3000 $ do
    Scotty.get "/" $ do
      res <- processRequest getSequenceNumber
      Scotty.json res

    Scotty.get "/dbs" $ do
      res <- processRequest getCtxDbs
      Scotty.json res

  return ()
  where
