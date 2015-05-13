module Continuum.Stream (module M
                        , collect) where

import Data.Stream.Monadic as M

collect :: (Monad m, Functor f, Traversable t) => f (t (m a)) -> f (m (t a))
collect = fmap sequence
