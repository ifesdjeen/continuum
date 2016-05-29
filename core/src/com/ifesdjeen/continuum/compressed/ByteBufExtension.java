package com.ifesdjeen.continuum.compressed;

import io.netty.buffer.ByteBuf;

/**
 * This class is completely thread-unsafe.
 *
 * Unflushed byte and/or all of it's bits will never be read from this buffer.
 *
 * TODO: there's a lot of room for optimisation here. This class is done
 * in the most barebone fashion. It's however possible to make it much more
 * performant by writing parts of integers into the buffer, flushing buffer
 * and writing the rest.
 */
public class ByteBufExtension {

    private final int BITS = 7;
    private final ByteBuf buffer;

    private byte unreadByte;
    private int unreadByteOffset = -1;

    private byte unwrittenByte;
    private int unwrittenByteOffset;

    public ByteBufExtension(ByteBuf buffer)
    {
        this.buffer = buffer;
    }

    /**
     * Writes the integer to the buffer, bitwise.
     */
    public void writeInt(int b)
    {
        writeBits(b, 32);
    }

    /**
     * Writes the top {@code bits} bits from the integer
     * to the buffer.
     */
    public void writeBits(int b, int bits)
    {
        for (int i = bits - 1; i >= 0; i--)
        {
            writeBit((b & (1 << i)) != 0x00);
        }
    }

    public int readInt()
    {
        return readBits(32);
    }

    public int readBits(int bits)
    {
        assert bits <= 32 && bits > 0;

        int result = 0;
        for (int i = bits - 1; i >= 0; i--)
        {
            if (readBit()) {
                result |= 1 << i;
            }
        }
        return result;
    }

    /**
     * Reads the single bit from the buffer.
     *
     * Reads the next byte from the buffer and reads it's bit
     * or reads the bit from the byte that was not completely consumed.
     */
    public boolean readBit()
    {
        if (unreadByteOffset < 0)
        {
            unreadByte = buffer.readByte();
            unreadByteOffset = 0;
        }

        boolean toReturn = (unreadByte & (1 << (BITS - unreadByteOffset))) != 0x00;
        unreadByteOffset += 1;

        if (unreadByteOffset == BITS)
        {
            unreadByteOffset = -1;
        }

        return toReturn;
    }

    /**
     * Writes a single bit to the buffer and flushes the byte buffer
     * if it's size overflows the byte size.
     *
     * For example, if 8 bits were written, the whole byte gets flushed
     * to the byte buffer;
     */
    public void writeBit(boolean bit)
    {
        writeBit(bit ? 1 : 0);
    }

    /**
     * Writes a single bit to the buffer
     */
    public void writeBit(int bit)
    {
        assert bit == 0 || bit == 1;
        unwrittenByte |= bit << (BITS - unwrittenByteOffset);
        unwrittenByteOffset += 1;

        if (unwrittenByteOffset == BITS)
        {
            unwrittenByteOffset = 0;
            buffer.writeByte(unwrittenByte);
            unwrittenByte = 0;
        }
    }

    /**
     * Fushes the last partially written bitstring / byte
     */
    public void flushUnwritten()
    {
        if (unwrittenByteOffset >= 0)
        {
            unwrittenByteOffset = 0;
            buffer.writeByte(unwrittenByte);
        }
    }

    /**
     * Outputs binary representation of the integer number
     */
    public static String toPrettyBinaryString(int number)
    {
        String s = "";
        for (int i = 31; i >= 0; i--)
        {
            if ((number & (1 << i)) == 0x00) {
                s += 0;
            }
            else
            {
                s += 1;
            }

            if (i % 4 == 0 && i > 0)
                s += " ";
        }
        return s;
    }
}
