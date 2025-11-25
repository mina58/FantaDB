package org.FantaDB.LibFanta.KV.Storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class FileAppendOnlyStorageTest {

    private Path tempFile;
    private FileAppendOnlyStorage storage;

    private void setup() throws IOException {
        tempFile = Files.createTempFile("fantadb-", ".log");
        storage = new FileAppendOnlyStorage(tempFile);
    }

    @AfterEach
    void cleanup() throws IOException {
        if (storage != null) storage.close();
        if (tempFile != null) Files.deleteIfExists(tempFile);
    }

    @Test
    void testAppendAndReadSimpleRecord() throws IOException {
        setup();

        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();

        long offset = storage.append(key, value, false);
        StorageRecord record = storage.read(offset);

        assertArrayEquals(key, record.key());
        assertArrayEquals(value, record.value());
        assertFalse(record.isTombstone());
    }

    @Test
    void testAppendMultipleRecords() throws IOException {
        setup();

        long off1 = storage.append("k1".getBytes(), "v1".getBytes(), false);
        long off2 = storage.append("k2".getBytes(), "v2".getBytes(), false);
        long off3 = storage.append("k3".getBytes(), "v3".getBytes(), false);

        assertEquals("v1", new String(storage.read(off1).value()));
        assertEquals("v2", new String(storage.read(off2).value()));
        assertEquals("v3", new String(storage.read(off3).value()));
    }

    @Test
    void testTombstoneRecord() throws IOException {
        setup();

        long offset = storage.append("dead".getBytes(), null, true);
        StorageRecord record = storage.read(offset);

        assertTrue(record.isTombstone());
    }

    @Test
    void testEmptyValue() throws IOException {
        setup();

        long offset = storage.append("key".getBytes(), new byte[0], false);
        StorageRecord record = storage.read(offset);

        assertNotNull(record.value());
        assertEquals(0, record.value().length);
    }

    @Test
    void testPersistenceAcrossReopen() throws IOException {
        setup();

        long offset = storage.append("persist".getBytes(), "ok".getBytes(), false);
        storage.flush();
        storage.close();

        // reopen
        storage = new FileAppendOnlyStorage(tempFile);

        StorageRecord record = storage.read(offset);
        assertEquals("persist", new String(record.key()));
        assertEquals("ok", new String(record.value()));
    }

    @Test
    void testOffsetsAreCorrect() throws IOException {
        setup();

        long off1 = storage.append("aa".getBytes(), "bb".getBytes(), false);
        long off2 = storage.append("cc".getBytes(), "dd".getBytes(), false);

        assertTrue(off2 > off1);

        StorageRecord r1 = storage.read(off1);
        StorageRecord r2 = storage.read(off2);

        assertEquals("aa", new String(r1.key()));
        assertEquals("cc", new String(r2.key()));
    }

    @Test
    void testIteratorIteratesOverAllRecordsInOrder() throws IOException {
        setup();

        long off1 = storage.append("k1".getBytes(), "v1".getBytes(), false);
        long off2 = storage.append("k2".getBytes(), "v2".getBytes(), false);
        long off3 = storage.append("k3".getBytes(), "v3".getBytes(), false);

        Iterator<StorageRecordOffset> it = storage.iterator();

        assertTrue(it.hasNext());
        StorageRecordOffset r1 = it.next();
        assertEquals(off1, r1.offset());
        assertEquals("k1", new String(r1.record().key()));
        assertEquals("v1", new String(r1.record().value()));

        assertTrue(it.hasNext());
        StorageRecordOffset r2 = it.next();
        assertEquals(off2, r2.offset());
        assertEquals("k2", new String(r2.record().key()));
        assertEquals("v2", new String(r2.record().value()));

        assertTrue(it.hasNext());
        StorageRecordOffset r3 = it.next();
        assertEquals(off3, r3.offset());
        assertEquals("k3", new String(r3.record().key()));
        assertEquals("v3", new String(r3.record().value()));

        assertFalse(it.hasNext());
    }

    @Test
    void testIteratorAfterReopen() throws IOException {
        setup();

        storage.append("a".getBytes(), "1".getBytes(), false);
        storage.append("b".getBytes(), "2".getBytes(), false);
        storage.flush();
        storage.close();

        // reopen
        storage = new FileAppendOnlyStorage(tempFile);

        StringBuilder sb = new StringBuilder();
        for (StorageRecordOffset off : storage) {
            sb.append(new String(off.record().key()))
                    .append(":")
                    .append(new String(off.record().value()))
                    .append(",");
        }

        assertEquals("a:1,b:2,", sb.toString());
    }

    @Test
    void testIteratorHandlesTombstones() throws IOException {
        setup();

        storage.append("x".getBytes(), "1".getBytes(), false);
        storage.append("y".getBytes(), null, true); // tombstone

        Iterator<StorageRecordOffset> it = storage.iterator();

        StorageRecordOffset r1 = it.next();
        assertFalse(r1.record().isTombstone());

        StorageRecordOffset r2 = it.next();
        assertTrue(r2.record().isTombstone());

        assertFalse(it.hasNext());
    }

    @Test
    void testIteratorWithEmptyValue() throws IOException {
        setup();

        storage.append("key".getBytes(), new byte[0], false);

        Iterator<StorageRecordOffset> it = storage.iterator();
        assertTrue(it.hasNext());

        StorageRecordOffset r = it.next();
        assertEquals("key", new String(r.record().key()));
        assertEquals(0, r.record().value().length);

        assertFalse(it.hasNext());
    }

    @Test
    void testIteratorOffsetsAreAccurate() throws IOException {
        setup();

        long off1 = storage.append("a".getBytes(), "111".getBytes(), false);
        long off2 = storage.append("b".getBytes(), "2222".getBytes(), false);

        Iterator<StorageRecordOffset> it = storage.iterator();

        StorageRecordOffset r1 = it.next();
        assertEquals(off1, r1.offset());

        StorageRecordOffset r2 = it.next();
        assertEquals(off2, r2.offset());
    }

    @Test
    void testIteratorHasNextEndsCorrectly() throws IOException {
        setup();

        storage.append("a".getBytes(), "1".getBytes(), false);

        Iterator<StorageRecordOffset> it = storage.iterator();

        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
    }

    @Test
    void testIterationOverRecords() throws IOException {
        setup();

        storage.append("k1".getBytes(), "v1".getBytes(), false);
        storage.append("k2".getBytes(), "v2".getBytes(), false);
        storage.append("k3".getBytes(), "v3".getBytes(), false);

        int count = 0;
        String[] expectedKeys = {"k1", "k2", "k3"};
        String[] expectedValues = {"v1", "v2", "v3"};

        int i = 0;
        for (StorageRecordOffset entry : storage) {
            assertEquals(expectedKeys[i], new String(entry.record().key()));
            assertEquals(expectedValues[i], new String(entry.record().value()));
            i++;
            count++;
        }

        assertEquals(3, count);
    }
}
