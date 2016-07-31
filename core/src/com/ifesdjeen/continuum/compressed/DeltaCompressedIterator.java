package com.ifesdjeen.continuum.compressed;

import java.util.Iterator;

public class DeltaCompressedIterator implements Iterator<DeltaCompressedIterator.Ts> {

    private final int LEADING_ZEROS_SIZE = 5;
    private final int MEANINGFUL_BITS_SIZE = 6;

    int valuesLeft;
    private final int values;
    private long lastTimestamp;
    private long lastValue;
    private int lastLeadingZeros = -1;
    private int lastMeaningfulBits = -1;
    private final ByteBufExtension buffer;
    public DeltaCompressedIterator(int values, ByteBufExtension buffer)
    {
        valuesLeft = values;
        this.values = values;
        this.buffer = buffer;
    }

    @Override
    public boolean hasNext() {
        return valuesLeft >= 0;
    }

    @Override
    public Ts next() {
        if (valuesLeft == values)
        {
            lastTimestamp = buffer.delegate().readLong();
            lastValue = buffer.delegate().readLong();
            valuesLeft--;
            return new Ts(lastTimestamp, Double.longBitsToDouble(lastValue));
        }
        else
        {
            boolean b1 = buffer.readBit();
            if (b1) {
                boolean b2 = buffer.readBit();
                if (b2) {
                    boolean b3 = buffer.readBit();
                    if (b3)
                    {
                        boolean b4 = buffer.readBit();
                        if (b4)
                        {
                            lastTimestamp += TimestampWriter.BITS_32.readDiff(buffer);
                        }
                        else
                        {
                            lastTimestamp += TimestampWriter.BITS_12.readDiff(buffer);
                        }
                    }
                    else
                    {
                        lastTimestamp += TimestampWriter.BITS_9.readDiff(buffer);
                    }
                }
                else
                {
                    long diff = TimestampWriter.BITS_7.readDiff(buffer);
                    lastTimestamp += diff;
                }
            }
            else
            {
                // ZERO, do nothing
            }

            boolean isDifferentValue = buffer.readBit();
            if (isDifferentValue)
            {
                boolean isSameMeaningfulBits = buffer.readBit();
                if (!isSameMeaningfulBits)
                {
                    lastLeadingZeros = buffer.readBits(LEADING_ZEROS_SIZE);
                    lastMeaningfulBits = buffer.readBits(MEANINGFUL_BITS_SIZE);
                }

                long xOrValue = buffer.readBitsAsLong(lastMeaningfulBits);
                lastValue ^= xOrValue << (64 - lastMeaningfulBits - lastLeadingZeros);
            }

            valuesLeft--;
            return new Ts(lastTimestamp, Double.longBitsToDouble(lastValue));
        }
    }

    public class Ts
    {
        public final long timestamp;
        public final double value;

        public Ts(long timestamp, double value)
        {
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Ts{" +
                    "timestamp=" + timestamp +
                    ", value=" + value +
                    '}';
        }
    }
}