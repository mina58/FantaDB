package org.FantaDB.LibFanta.KV.Storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
}
