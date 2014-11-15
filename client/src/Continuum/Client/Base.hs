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

data ContinuumClient = ContinuumClient { clientSocket :: N.Socket N.Bus }

connect :: String
           -> String
           -> IO ContinuumClient

connect host port = do
  socket <- N.socket N.Bus
  _      <- N.connect socket ("tcp://" ++ host ++ ":" ++ port)
  return ContinuumClient { clientSocket = socket }

executeQuery :: ContinuumClient
             -> Query
             -> IO (DbErrorMonad DbResult)
executeQuery client query = do
  let sock = (clientSocket client)
  -- It's a request query
  _      <-  N.send sock (encode (Query query))
  bs     <-  N.recv sock
  return $ (S.decodeDbResult bs)
