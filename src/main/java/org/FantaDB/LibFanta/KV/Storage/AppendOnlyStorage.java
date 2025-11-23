package org.FantaDB.LibFanta.KV.Storage;

import java.io.Closeable;
import java.io.IOException;

public interface AppendOnlyStorage extends Closeable {
    // Append a new record to storage and return its location (offset)
    long append(byte[] key, byte[] value, boolean isTombstone) throws IOException;

    // Read a record from a known location
    StorageRecord read(long offset) throws IOException;

    // Force data to disk (WAL safety)
    void flush() throws IOException;
}
