{-# LANGUAGE CPP              #-}
{-# LANGUAGE DeriveGeneric    #-}
{-# LANGUAGE FlexibleContexts #-}

module Continuum.Client.Base
       (module Continuum.Client.Base
        , module Continuum.Types
        )
       where

import qualified Nanomsg as N
import           Data.Serialize (encode, decode)
import qualified Continuum.Serialization.Base as S
import           Continuum.Types

data ContinuumClient = ContinuumClient
    { clientSocket   :: N.Socket N.Req
    , clientEndpoint :: N.Endpoint }

connect :: String
           -> String
           -> IO ContinuumClient

connect host port = do
  socket   <- N.socket N.Req
  endpoint <- N.connect socket ("tcp://" ++ host ++ ":" ++ port)
  return ContinuumClient { clientSocket   = socket
                         , clientEndpoint = endpoint}

disconnect :: ContinuumClient -> IO ()
disconnect client = do
  N.shutdown (clientSocket client) (clientEndpoint client)
  N.close (clientSocket client)
  return ()

executeQuery :: ContinuumClient
             -> DbName
             -> SelectQuery
             -> IO (DbErrorMonad DbResult)
executeQuery client dbName query = do
  let sock = (clientSocket client)
  _      <-  N.send sock (encode (Select dbName query))
  bs     <-  N.recv sock
  return $ (S.decodeDbResult bs)

sendRequest :: ContinuumClient
             -> Request
             -> IO (DbErrorMonad DbResult)
sendRequest client request = do
  let sock = (clientSocket client)
  _      <-  N.send sock (encode request)
  bs     <-  N.recv sock
  return $ (S.decodeDbResult bs)
