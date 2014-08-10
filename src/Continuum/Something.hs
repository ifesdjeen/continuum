module Continuum.Something where

newtype ReaderIO s a = ReaderIO { runReaderIO :: s -> IO a }
