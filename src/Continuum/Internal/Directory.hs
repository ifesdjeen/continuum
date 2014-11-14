{-# LANGUAGE CPP, ForeignFunctionInterface #-}

module Continuum.Internal.Directory where

import           Foreign
import           Foreign.C.Types
import           Foreign.C.String

-- |Create a directory
--
mkdir :: String -> Int -> IO ()
mkdir path mode = do
  cPath <- newCString path
  _     <- __mkdir cPath (fromIntegral mode)
  return ()

-- -------------------- --
-- Auxilitary functions --
-- -------------------- --

foreign import ccall unsafe "sys/stat.h mkdir" __mkdir  :: CString  -> CUInt -> IO CInt
