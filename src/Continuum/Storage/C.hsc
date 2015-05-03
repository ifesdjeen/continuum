{-# LANGUAGE CPP               #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE BangPatterns #-}

module Continuum.Storage.C where

makeStepFun :: IORef (DbErrorMonad x) -> Decoder a -> (x -> a -> x) -> StepFun
makeStepFun ref decoder step keyPtr keyLen valPtr valLen = do
  key <- decodeString keyPtr keyLen
  val <- decodeString valPtr valLen
  modifyIORef' ref (\prev -> step <$> prev <*> (decoder (key, val))) -- Do we need forcing seq here?

-- |
-- | Foreign imports
-- |

foreign import ccall safe "continuum.h scan_range"
  c_scan_range :: LevelDBPtr
               -> ReadOptionsPtr
               -> CString
               -> CSize
               -> CString
               -> CSize
               -> FunPtr StepFun
               -> CInt
               -> IO ()

foreign import ccall safe "wrapper" makeCStepFun :: StepFun -> IO StepFunPtr
-- |
-- | Types
-- |

type StepFun = CString -> CSize -> CString -> CSize -> IO ()
type StepFunPtr = FunPtr StepFun
