# Continuum

Continuum is a prototype of a time-series database, using `Google Leveldb` written in Haskell.

This is a research / experimental project and I recommend not using it in production.
If you want experiment and search for ideas on time series and streamed data processing,
this might be what you want.

## Querying

Queries are implemented using ideas of `Stream Fusion`. In simple words, stream fusion
allows combining a multi-step streamed operation into a single step operation. For
example, if you have a `map-filter-reduce` operation, it could be represented
as one `map-filter-reduce` step, where `reduce` keeps intermediate results of aggregation.

It's different from the "traditional" approach is that when, for example, you have an
incoming set of numbers `[1,2,3,4]`, and you'd like to map, filter and reduce them, you'd
have to first map an entire array first, then run a filter step and only then start
reducing. This means that during the operations all the intermediate step results have
to be preserved in memory, which is very inneficient.

Stream is represented by it's `Step`, an `Iterator` (in other words, `Pointer`) and
a function that can yield `Step` from it:

```hs
data Step   a  s
   = Yield  a !s
   | Skip  !s
   | Done
   
data Stream m a = forall s. Stream (s -> m (Step a s)) (m s)
```

`map` operation would look like that:

```hs
map :: Monad m => (a -> b) -> Stream m a -> Stream m b
map f (Stream next0 s0) = Stream next s0
  where
    next !s = do
        step <- next0 s
        return $ case step of
            Done       -> Done
            Skip    s' -> Skip        s'
            Yield x s' -> Yield (f x) s'
```

As you can see, `Yield` simply transforms `Stream` of `a` into
the `Stream` of `b`, the other operations leave the `Step`
unchanged.

`filter`, instead, would either `Yield` the item, if it matches
the predicate or pass a `Skip` over to the next operations. 
```hs
filter :: Monad m => (a -> Bool) -> Stream m a -> Stream m a
filter p (Stream next0 s0) = Stream next s0
  where
    next !s = do
        step <- next0 s
        return $ case step of
            Done                   -> Done
            Skip    s'             -> Skip    s'
            Yield x s' | p x       -> Yield x s'
                       | otherwise -> Skip    s'
```

## Query Parallelism

In order to achieve query parallelism, we keep `Chunk` pointers
in the database. After some configurable amount of records is
written into the database, the timestamp of the last inserted
record is put into the `Chunks` table.

## Data Layout 

```
0                8                16
+----------------+----------------+----------------+ ...
| field 1 offset | field 2 offset | field 3 offset | ...
+----------------+----------------+----------------+ ...
 
    24             24 + offset    24 + offset
... +--------------+--------------+--------------+
... | field 1 data | field 2 data | field 2 data |
... +--------------+--------------+--------------+
```


# License

Copyright © 2014 Alex Petrov 

Continuum is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
