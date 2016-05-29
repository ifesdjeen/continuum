package com.ifesdjeen.continuum.compressed;

import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ByteBufferExtension {

    @Test
    public void testReadBit() {
        ByteBufExtension buf = new ByteBufExtension(Unpooled.buffer(8));

        buf.writeBit(true);
        buf.writeBit(false);
        buf.writeBit(true);
        buf.writeBit(false);
        buf.writeBit(true);
        buf.writeBit(false);
        buf.writeBit(true);
        buf.writeBit(false);

        buf.flushUnwritten();

        assertTrue(buf.readBit());
        assertFalse(buf.readBit());
        assertTrue(buf.readBit());
        assertFalse(buf.readBit());
        assertTrue(buf.readBit());
        assertFalse(buf.readBit());
        assertTrue(buf.readBit());
        assertFalse(buf.readBit());
    }


    @Test
    public void testReadBits() {
        ByteBufExtension buf = new ByteBufExtension(Unpooled.buffer(8));

        for (int i = 0; i < 100; i++)
        {
            buf.writeBit(i % 2 == 0);
        }

        buf.flushUnwritten();

        for (int i = 0; i < 100; i++)
        {
            assertEquals(i % 2 == 0, buf.readBit());
        }
    }

    @Test
    public void testReadInt()
    {
        ByteBufExtension buf = new ByteBufExtension(Unpooled.buffer(8));
        buf.writeBit(true);
        buf.writeBit(false);
        buf.writeBit(true);
        buf.writeBit(false);
        buf.writeInt(12345);
        buf.writeBit(true);
        buf.writeBit(false);
        buf.writeBit(true);

        buf.flushUnwritten();

        assertTrue(buf.readBit());
        assertFalse(buf.readBit());
        assertTrue(buf.readBit());
        assertFalse(buf.readBit());
        assertEquals(12345, buf.readInt());
        assertTrue(buf.readBit());
        assertFalse(buf.readBit());
        assertTrue(buf.readBit());
    }

    @Test
    public void testBinaryString()
    {
        assertEquals("0000 0000 0000 0000 0011 0000 0011 1001",
                ByteBufExtension.toPrettyBinaryString(12345));
    }
}
