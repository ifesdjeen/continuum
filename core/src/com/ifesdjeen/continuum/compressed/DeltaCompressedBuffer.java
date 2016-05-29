package com.ifesdjeen.continuum.compressed;

import io.netty.buffer.ByteBuf;

import java.util.Iterator;

/**
 * Delta compression algorithm.
 *
 */
public class DeltaCompressedBuffer
{
    private final ByteBufExtension buffer;
    private final int LEADING_ZEROS_SIZE = 5;
    private final int MEANINGFUL_BITS_SIZE = 6;
    private long lastTimestamp;
    private long lastValue;
    private int lastLeadingZeroes = -1;
    private int lastTrailingZeroes = -1;
    private int values;

    public DeltaCompressedBuffer(ByteBuf buffer, long timestamp, double firstValue)
    {
        this.buffer = new ByteBufExtension(buffer);
        writeHeader(buffer, timestamp, firstValue);
    }

    private void writeHeader(ByteBuf buf, long timestamp, double firstValue)
    {
        lastTimestamp = timestamp;
        lastValue = Double.doubleToLongBits(firstValue);
        buf.writeLong(timestamp);
        buf.writeLong(lastValue);
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
            System.out.println("ABSOLUTELY SAME VALUE");
            buffer.writeBit(0);
        }
        else {
            buffer.writeBit(1);

            final int leadingZeros = Long.numberOfLeadingZeros(xOrValue);
            final int trailingZeros = Long.numberOfTrailingZeros(xOrValue);
            final int meaningfulBits = 64 - leadingZeros - trailingZeros;

            if (leadingZeros == lastLeadingZeroes && trailingZeros == lastTrailingZeroes) {
                buffer.writeBit(1); // amount of meaningful bits is same
            } else {
                buffer.writeBit(0); // amount of meaningful bits is different
                buffer.writeBits(leadingZeros, LEADING_ZEROS_SIZE);
                buffer.writeBits(meaningfulBits, MEANINGFUL_BITS_SIZE); // meaningful bits
            }

            long valueToWrite = xOrValue >>> trailingZeros;
            System.out.println("LAST VALUE    " + ByteBufExtension.toPrettyBinaryString(lastValue));
            System.out.println("CURRENT VALUE " + ByteBufExtension.toPrettyBinaryString(currentValue));
            System.out.println("XOR VALUE " + xOrValue);
            System.out.println("XOR VALUE " + ByteBufExtension.toPrettyBinaryString(xOrValue));
            System.out.println("SHIFTED   " + ByteBufExtension.toPrettyBinaryString(valueToWrite));
            System.out.println("LEADING ZEROS " + leadingZeros + " TRAILING ZEROS " + trailingZeros + " MEANINGFUL " + meaningfulBits);
            buffer.writeBits(valueToWrite, meaningfulBits); // store meaningful bits

            // TODO REMOVE
            this.lastLeadingZeroes = leadingZeros;
            this.lastTrailingZeroes = trailingZeros;
        }

        values++;
        this.lastTimestamp = timestamp;
        this.lastValue = currentValue;
    }

    public void close()
    {
        buffer.flushUnwritten();
    }

    public DeltaCompressedIterator iterator()
    {
        return new DeltaCompressedIterator(values, buffer);
    }

    public TimestampWriter getTimestampWriter(long value)
    {
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