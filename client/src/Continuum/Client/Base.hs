{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}

module Continuum.Client.Base
       (module Continuum.Client.Base
        , module Continuum.Common.Types
        )
       where

import qualified Nanomsg as N
import           Data.Serialize (encode, decode)
import qualified Continuum.Common.Serialization as S
import           Continuum.Common.Types

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
             -> Query
             -> IO (DbErrorMonad DbResult)
executeQuery client query = do
  let sock = (clientSocket client)
  _      <-  N.send sock (encode (RunQuery query))
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
