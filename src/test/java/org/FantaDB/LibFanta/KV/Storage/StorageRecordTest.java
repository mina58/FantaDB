package org.FantaDB.LibFanta.KV.Storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StorageRecordTest {
    @Test
    void testToStringWithNormalValue() {
        StorageRecord record = new StorageRecord("k".getBytes(), "v".getBytes(), false);
        String str = record.toString();
        assertTrue(str.contains("key=k"));
        assertTrue(str.contains("value=v"));
        assertTrue(str.contains("tombstone=false"));
    }

    @Test
    void testToStringWithEmptyValue() {
        StorageRecord record = new StorageRecord("k".getBytes(), new byte[0], false);
        String str = record.toString();
        assertTrue(str.contains("key=k"));
        assertTrue(str.contains("value=")); // empty string
        assertTrue(str.contains("tombstone=false"));
    }

    @Test
    void testToStringWithNullValue() {
        StorageRecord record = new StorageRecord("k".getBytes(), null, false);
        String str = record.toString();
        assertTrue(str.contains("key=k"));
        assertTrue(str.contains("value=null"));
        assertTrue(str.contains("tombstone=false"));
    }

    @Test
    void testToStringWithTombstone() {
        StorageRecord record = new StorageRecord("k".getBytes(), null, true);
        String str = record.toString();
        assertTrue(str.contains("key=k"));
        assertTrue(str.contains("value=null"));
        assertTrue(str.contains("tombstone=true"));
    }
}