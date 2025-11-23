package org.FantaDB.LibFanta.KV.Storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class StorageRecordCodecTest {
    private final StorageRecordCodec codec = new StorageRecordCodec();

    @Test
    void testRoundTrip() {
        StorageRecord record = new StorageRecord("key".getBytes(), "value".getBytes(), false);
        byte[] bytes = codec.serialize(record);
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 4, bytes.length - 4); // skip total length
        StorageRecord decoded = codec.deserialize(buffer);

        assertArrayEquals(record.key(), decoded.key());
        assertArrayEquals(record.value(), decoded.value());
        assertEquals(record.isTombstone(), decoded.isTombstone());
    }

    @Test
    void testEmptyValue() {
        StorageRecord record = new StorageRecord("key".getBytes(), new byte[0], false);
        byte[] bytes = codec.serialize(record);
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 4, bytes.length - 4);
        StorageRecord decoded = codec.deserialize(buffer);

        assertArrayEquals(record.key(), decoded.key());
        assertNotNull(decoded.value());
        assertEquals(0, decoded.value().length);
        assertFalse(decoded.isTombstone());
    }

    @Test
    void testTombstone() {
        StorageRecord record = new StorageRecord("key".getBytes(), null, true);
        byte[] bytes = codec.serialize(record);
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 4, bytes.length - 4);
        StorageRecord decoded = codec.deserialize(buffer);

        assertArrayEquals(record.key(), decoded.key());
        assertNotNull(decoded.value()); // your format uses byte[0] for nulls/empty
        assertEquals(0, decoded.value().length);
        assertTrue(decoded.isTombstone());
    }

    @Test
    void testNonAscii() {
        String keyStr = "مفتاح🚀";
        String valueStr = "قيمة🔥";

        StorageRecord record = new StorageRecord(keyStr.getBytes(), valueStr.getBytes(), false);
        byte[] bytes = codec.serialize(record);
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 4, bytes.length - 4);
        StorageRecord decoded = codec.deserialize(buffer);

        assertArrayEquals(record.key(), decoded.key());
        assertArrayEquals(record.value(), decoded.value());
        assertFalse(decoded.isTombstone());
    }

    @Test
    void testMultipleRecords() {
        StorageRecord[] records = {
                new StorageRecord("k1".getBytes(), "v1".getBytes(), false),
                new StorageRecord("k2".getBytes(), "v2".getBytes(), false),
                new StorageRecord("k3".getBytes(), new byte[0], false),
        };

        for (StorageRecord rec : records) {
            byte[] bytes = codec.serialize(rec);
            ByteBuffer buffer = ByteBuffer.wrap(bytes, 4, bytes.length - 4);
            StorageRecord decoded = codec.deserialize(buffer);

            assertArrayEquals(rec.key(), decoded.key());
            assertArrayEquals(rec.value(), decoded.value());
            assertEquals(rec.isTombstone(), decoded.isTombstone());
        }
    }
}
