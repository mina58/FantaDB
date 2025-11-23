package org.FantaDB.LibFanta.KV.Storage;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

// Storage format
// [total length: 4 bytes]
// [tombstone: 1 byte]
// [key length: 4 bytes]
// [key value: variable]
// [value length: 4 bytes]
// [value value: variable]
public class StorageRecordCodec {
    public byte @NotNull [] serialize(@NotNull StorageRecord record) {
        int keyLength = record.key().length;
        int valueLength = record.value() != null ? record.value().length : 0;

        // Record length = tombstone(1) + keyLength(4) + key bytes + valueLength(4) + value bytes
        int recordLength = 1 + 4 + keyLength + 4 + valueLength;

        // 4 (for the total length) + record length
        ByteBuffer buffer = ByteBuffer.allocate(4 + recordLength);

        buffer.putInt(recordLength);
        buffer.put((byte) (record.isTombstone() ? 1 : 0));
        buffer.putInt(keyLength);
        buffer.put(record.key());
        buffer.putInt(valueLength);
        if (valueLength > 0) {
            buffer.put(record.value());
        }

        return buffer.array();
    }

    public StorageRecord deserialize(@NotNull ByteBuffer buffer) {
        boolean tombstone = buffer.get() == 1;
        int keyLength = buffer.getInt();
        byte[] key = new byte[keyLength];
        buffer.get(key);
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new StorageRecord(key, value, tombstone);
    }
}
