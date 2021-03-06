package com.ifesdjeen.continuum.compressed;

import io.netty.buffer.ByteBuf;

/**
 * Implementation of Gorilla compression algorithm.
 */
// TODO: rename to bucket
// TODO: possibly on-disk buckets, too?
// TODO: memory-mapped buffers?
public class DeltaCompressedBuffer
{
    private final int LEADING_ZEROS_SIZE = 5;
    private final int MEANINGFUL_BITS_SIZE = 6;

    private final ByteBufExtension buffer;

    // TODO: move to contructor
    private volatile long firstTimestamp;
    private volatile long lastTimestamp;
    private volatile int valuesCount;

    private long lastValue;
    private int lastLeadingZeroes = -1;
    private int lastTrailingZeroes = -1;


    public DeltaCompressedBuffer(ByteBuf buffer)
    {
        // TODO: align size to power of 2
        this.buffer = new ByteBufExtension(buffer);
    }

    public void writeHeader(long timestamp, double firstValue)
    {
        firstTimestamp = timestamp;
        lastTimestamp = timestamp;
        lastValue = Double.doubleToLongBits(firstValue);
        buffer.delegate().writeLong(timestamp);
        buffer.delegate().writeLong(lastValue);
    }

    public void append(long timestamp, double value)
    {
        // assert monotonicity
        assert timestamp > lastTimestamp;
        final int timestampDelta = (int) (timestamp - lastTimestamp);

        getTimestampWriter(timestampDelta).writeDiff(buffer, timestampDelta);

        final long currentValue = Double.doubleToLongBits(value);
        final long xOrValue = lastValue ^ currentValue;

        if (xOrValue == 0x00)
        {
            // TODO: add proper boundary checks
            buffer.writeBit(0); // valuesCount are absolutely same
        }
        else {
            final int leadingZeros = Long.numberOfLeadingZeros(xOrValue);
            final int trailingZeros = Long.numberOfTrailingZeros(xOrValue);
            final int meaningfulBits = 64 - leadingZeros - trailingZeros;
            buffer.writeBit(1); // valuesCount are not the same

            if (leadingZeros == lastLeadingZeroes && trailingZeros == lastTrailingZeroes) {
                buffer.writeBit(1); // amount of meaningful bits is same
            } else {
                buffer.writeBit(0); // amount of meaningful bits is different
                buffer.writeBits(leadingZeros, LEADING_ZEROS_SIZE);
                buffer.writeBits(meaningfulBits, MEANINGFUL_BITS_SIZE);
            }

            long valueToWrite = xOrValue >>> trailingZeros;
            buffer.writeBits(valueToWrite, meaningfulBits);

            this.lastLeadingZeroes = leadingZeros;
            this.lastTrailingZeroes = trailingZeros;
        }

        valuesCount++;
        this.lastTimestamp = timestamp;
        this.lastValue = currentValue;
    }

    public int size() {
        return valuesCount;
    }

    public long firstTimestamp() {
        return firstTimestamp;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public void close()
    {
        buffer.flushUnwritten();
    }

    public DeltaCompressedIterator iterator()
    {
        return new DeltaCompressedIterator(valuesCount, buffer);
    }

    /**
     * Figure out which timestamp writer to use
     */
    public TimestampWriter getTimestampWriter(long value)
    {
        // No difference, single bit
        if (value == 0)
            return TimestampWriter.ZERO;

        // + 7 bits value
        if (value >= -63 && value <= 64)
            return TimestampWriter.BITS_7;

        // + 9 bits value
        if (value >= -255 && value <= 256)
            return TimestampWriter.BITS_9;

        // + 12 bits value
        if (value >= -2047 && value <= 2048)
            return TimestampWriter.BITS_12;

        // 32 bits value
        return TimestampWriter.BITS_32;
    }

}