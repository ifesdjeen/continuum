{-# LANGUAGE OverloadedStrings #-}

module Continuum.Service.HttpService where

import           Control.Concurrent.STM
import           Control.Concurrent             ( forkIO )
import           Control.Monad.State.Strict     ( evalStateT )
import qualified Web.Scotty as Scotty
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Applicative            ( (<$>) )
import           Continuum.Types

import           Data.ByteString                ( ByteString )
import           Data.ByteString.Char8          ( split )

data DbContext = DbContext
    { ctxSystemDb    :: DB
    , ctxDbs         :: ContextDbsMap
    , ctxChunksDb    :: DB
    }

runWebServer :: (TVar DbContext) -> IO ()
runWebServer ctxTVar = do
  _ <- forkIO $ Scotty.scotty 3000 $ do

    -- TODO
    Scotty.get "/" $ do
      Scotty.json []

  return ()
  where
