{-# LANGUAGE OverloadedStrings #-}

module Continuum.Service.HttpService where

import           Control.Concurrent.STM
import           Control.Concurrent             ( forkIO )
import           Control.Monad.State.Strict     ( evalStateT )
import           Control.Monad.Catch            ( MonadMask(..) )
import           Control.Monad.Trans.Class      ( lift )
import qualified Web.Scotty as Scotty
import qualified Control.Exception as ControlException
import           Web.Scotty.Internal.Types ( ScottyT )
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Applicative            ( (<$>) )
import           Continuum.Types
import           Continuum.Storage.SystemStorage as SysStorage

import           Data.ByteString                ( ByteString )
import           Data.ByteString.Char8          ( split )

data DbContext = DbContext
    { ctxSystemDb    :: DB
    , ctxChunksDb    :: DB
    }


runWebServer :: (TVar DbContext) -> IO ()
runWebServer ctxTVar = do
  let path = "/tmp/continuum-test-db"
  _ <- forkIO $
       withDb path "system" $ \systemDb ->
       withDb path "chunks" $ \chunksDb ->
       Scotty.scotty 3000 $

    -- TODO
    Scotty.get "/" $ do
      Scotty.json (123 :: Integer)

  return ()
  where
