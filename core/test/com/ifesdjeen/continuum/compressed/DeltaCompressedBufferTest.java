package com.ifesdjeen.continuum.compressed;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeltaCompressedBufferTest
{
    private static Random random;

    @BeforeClass
    public static void setup()
    {
        long seed = System.nanoTime();
        random = new Random(seed);
        System.out.println("SEED: " + seed);
    }

    // TODO: calculate compression rate for random values, prolly not the most useful tho

    @Test
    public void testDoubleRandom()
    {
        final int items = 10;
        final double[] values = new double[items];
        final int[] tsDeltas = new int[items];

        for (int i = 0; i < items; i++) {
            values[i] = random.nextDouble();
            tsDeltas[i] = Math.abs(random.nextInt());
        }

        final ByteBuf buf = Unpooled.buffer(1024);
        final long baseTs = 10000L;

        DeltaCompressedBuffer compressor = new DeltaCompressedBuffer(buf);
        long ts = baseTs;
        for (int i = 0; i < items; i++) {
            ts += tsDeltas[i];
            if (i == 0)
            {
                compressor.writeHeader(ts, values[i]);
            }
            else
            {
                compressor.append(ts, values[i]);
            }
        }

        compressor.close();

        DeltaCompressedIterator iterator = compressor.iterator();
        ts = baseTs;
        for (int i = 0; i < items; i++) {
            assertTrue(iterator.hasNext());
            DeltaCompressedIterator.Ts res = iterator.next();
            ts += tsDeltas[i];
            assertEquals(ts, res.timestamp);
            assertEquals(values[i], res.value, 0.00);
        }

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWholeNumbers()
    {
        ByteBuf buf = Unpooled.buffer(1024);
        DeltaCompressedBuffer compressor = new DeltaCompressedBuffer(buf);
        compressor.writeHeader(10000L, 12);
        compressor.append(10001L, 24);
        compressor.append(10002L, 15);
        compressor.append(10003L, 12);
        compressor.append(10004L, 35);

        compressor.close();

        DeltaCompressedIterator iter = compressor.iterator();
        DeltaCompressedIterator.Ts ts = iter.next();
        assertEquals(10000L, ts.timestamp);
        assertEquals(12, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10001L, ts.timestamp);
        assertEquals(24, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10002L, ts.timestamp);
        assertEquals(15, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10003L, ts.timestamp);
        assertEquals(12, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10004L, ts.timestamp);
        assertEquals(35, ts.value, 0.0);
    }

    @Test
    public void testWithFloatingPoint()
    {
        ByteBuf buf = Unpooled.buffer(1024);
        DeltaCompressedBuffer compressor = new DeltaCompressedBuffer(buf);
        compressor.writeHeader(10000L, 15.5);
        compressor.append(10001L, 14.0625);
        compressor.append(10002L, 3.25);
        compressor.append(10003L, 8.625);
        compressor.append(10004L, 13.1);

        compressor.close();

        DeltaCompressedIterator iter = compressor.iterator();
        DeltaCompressedIterator.Ts ts = iter.next();
        assertEquals(10000L, ts.timestamp);
        assertEquals(15.5, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10001L, ts.timestamp);
        assertEquals(14.0625, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10002L, ts.timestamp);
        assertEquals(3.25, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10003L, ts.timestamp);
        assertEquals(8.625, ts.value, 0.0);

        ts = iter.next();
        assertEquals(10004L, ts.timestamp);
        assertEquals(13.1, ts.value, 0.0);
    }
}
