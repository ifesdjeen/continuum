package com.ifesdjeen.continuum.compressed;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Iterators;
import io.netty.buffer.Unpooled;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class Store {

    public static int bytesize = 128;

    // TODO: use tree instead of list to navigate through read buckets better
    ConcurrentMap<String, List<DeltaCompressedBuffer>> readBuckets;
    ConcurrentMap<String, DeltaCompressedBuffer> writeBucket;

    public Store() {
        readBuckets = new NonBlockingHashMap();
        writeBucket = new NonBlockingHashMap<>();
    }

    // TODO: this all is highly tread-unsafe
    public void insert(String metric, long timestamp, double value) {
        DeltaCompressedBuffer bucket = writeBucket.get(metric);

        if (bucket == null) {
            bucket = new DeltaCompressedBuffer(Unpooled.buffer(bytesize, bytesize));
            bucket.writeHeader(timestamp, value);
            writeBucket.putIfAbsent(metric, bucket);
        } else if (bucket.size() == 128) {
            readBuckets.get(metric).add(bucket);
            // switch current write bucket
            bucket = new DeltaCompressedBuffer(Unpooled.buffer(bytesize, bytesize));
            bucket.writeHeader(timestamp, value);
            writeBucket.putIfAbsent(metric, bucket);
        } else {
            bucket.append(timestamp, value);
        }
    }


    // TODO: aggregate-iteration
    public Iterator<DeltaCompressedIterator.Ts> fetch(String metric, long from, long to) {
        List<DeltaCompressedBuffer> history = readBuckets.get(metric);
        // TODO: rewrite with peekingiterator.
        Iterator<DeltaCompressedBuffer> historyIterators = history == null ? Iterators.emptyIterator() : history.iterator();

        Iterator<DeltaCompressedIterator> iter = new Iterator<DeltaCompressedIterator>() {
            @Override
            public boolean hasNext() {
                while (historyIterators.hasNext()) {
                    if (historyIterators.)
                }

                return false;
            }

            @Override
            public DeltaCompressedIterator next() {
                return null;
            }
        }

        return new Iterator<DeltaCompressedIterator.Ts>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public DeltaCompressedIterator.Ts next() {
                return null;
            }
        }
    }

}
