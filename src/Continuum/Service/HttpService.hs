{-# LANGUAGE OverloadedStrings #-}

module Continuum.Service.HttpService where

import           Data.Text.Lazy                 ( Text )
import           Control.Concurrent.STM         ( TVar, atomically, readTVar, writeTVar, newTVar, modifyTVar)
import           Control.Concurrent             ( forkIO )
import           Control.Monad.State.Strict     ( evalStateT )
import           Control.Monad.Catch            ( MonadMask(..) )

import qualified Data.Map.Strict as Map
import qualified Data.ByteString.Char8  as BS
import qualified Web.Scotty as Scotty
import qualified Control.Exception as ControlException
import qualified Data.Aeson as Json
import           Web.Scotty ( ActionM(..), ScottyM(..) )
import           Web.Scotty.Internal.Types ( ScottyT )
import           Control.Monad.IO.Class         ( liftIO )
import           Control.Applicative            ( (<$>) )
import           Continuum.Types
import           Continuum.Storage.SystemStorage as SysStorage

import           Data.ByteString                ( ByteString )
import           Data.ByteString.Char8          ( split )
import           Network.HTTP.Types             ( status404 )

data DbContext = DbContext
    { ctxSystemDb    :: DB
    , ctxChunksDb    :: DB
    }


runWebServer :: (TVar DbContext) -> IO ()
runWebServer ctxTVar = do
  let path = "/tmp/continuum-test-db"
  _ <- forkIO $

       withDb path "system" $ \systemDb -> do
         schemas     <- fetchDbs systemDb
         let dbNames = map fst schemas
         dbInstances <- mapM (openDb path) $ dbNames
         dbState <- atomInit $ Map.fromList $ zip dbNames (zip dbInstances (map snd schemas))

         withDb path "chunks" $ \chunksDb ->
           Scotty.scotty 3000 $ do

             Scotty.get "/dbs" $ do
               liftIO $ print $ "asd"
               Scotty.json (123 :: Integer)

             Scotty.post "/dbs" $ do
               dbName    <- Scotty.param "name"
               schemaStr <- Scotty.param "schema"

               case (Json.decode schemaStr) of
                (Just schema) -> do
                  _  <- liftIO $ createDatabase systemDb dbName schema
                  db <- liftIO $ openDb path dbName
                  _  <- liftIO $ atomSwap (Map.insert dbName (db, schema)) dbState

                  Scotty.json ("ok" :: Text)
                (Nothing)     -> Scotty.json ("ok" :: Text)

             Scotty.post "/dbs/:dbName" $ do
               dbs <- liftIO $ atomRead dbState
               Scotty.json $ map (\(a, (_,b)) -> (a,b)) (Map.toList dbs)

             Scotty.get "/dbs/:dbName/:timestamp" $ do
               Scotty.json (123 :: Integer)

             Scotty.get "/" $ do
               Scotty.json (123 :: Integer)

  return ()




--- TODO: "Just OR 404"
registerDbHandlers :: String -> DbName -> DbSchema -> IO (ScottyM ())
registerDbHandlers path dbName schema = do
  db <- openDb path dbName
  return $ Scotty.get (Scotty.literal $ "/" ++ (BS.unpack dbName) ++ "/") $
    Scotty.json (123 :: Integer)


-- |
-- | Atom-related
-- |

atomRead :: TVar a -> IO a
atomRead = atomically . readTVar

atomSwap :: (b -> b) -> TVar b -> IO ()
atomSwap f x = atomically $ modifyTVar x f

atomReadSwap :: (b -> b) -> TVar b -> IO b
atomReadSwap f x = atomically $ do
  v <- readTVar x
  _ <- modifyTVar x f
  return v

atomReset :: b -> TVar b -> IO ()
atomReset newv x = atomically $ writeTVar x newv

atomInit :: b -> IO (TVar b)
atomInit v = atomically $ newTVar v

-- |
-- | Scotty Helpers
-- |

maybeParam :: Scotty.Parsable a => Text -> Scotty.ActionM (Maybe a)
maybeParam p = (Just <$> Scotty.param p) `Scotty.rescue` (\_ -> (return Nothing))
