package org.FantaDB.LibFanta.KV.Engine;

public interface KVEngine {
    void put(String key, String value);
    String get(String key);
    void remove(String key);
}
