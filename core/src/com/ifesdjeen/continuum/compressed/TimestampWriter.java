package com.ifesdjeen.continuum.compressed;

public enum TimestampWriter
{
    ZERO    ((byte) 0,    1, 1),  // 0000
    BITS_7  ((byte) 0x02, 2, 7),  // 0010
    BITS_9  ((byte) 0x06, 3, 9),  // 0100
    BITS_12 ((byte) 0x0e, 4, 12), // 1110
    BITS_32 ((byte) 0x0f, 4, 32); // 1111

    private final byte bits;
    private final int bitSize;
    private final int valueSize;

    TimestampWriter(byte bits, int bitSize, int valueSize)
    {
        this.bits = bits;
        this.bitSize = bitSize;
        this.valueSize = valueSize;
    }

    public void writeDiff(ByteBufExtension byteBuf, int value)
    {
        byteBuf.writeBits(bits, bitSize);
        byteBuf.writeBits(value, valueSize);
    }

    public long readDiff(ByteBufExtension byteBufExtension)
    {
        assert bits != 0x00; // default case;
        return byteBufExtension.readBits(valueSize);
    }
}