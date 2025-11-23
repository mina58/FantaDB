package org.FantaDB.LibFanta.KV.Storage;

import org.jetbrains.annotations.NotNull;

public record StorageRecord(byte[] key, byte[] value, boolean isTombstone) {
    @Override
    public @NotNull String toString() {
        String keyStr = new String(key);
        String valueStr = value != null ? new String(value) : "null";
        return "StorageRecord{" +
                "key=" + keyStr +
                ", value=" + valueStr +
                ", tombstone=" + isTombstone +
                '}';
    }
}

