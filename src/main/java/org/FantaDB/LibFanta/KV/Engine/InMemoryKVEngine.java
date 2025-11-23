package org.FantaDB.LibFanta.KV.Engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKVEngine implements KVEngine {
    private final Map<String,String> map = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        map.put(key, value);
    }

    @Override
    public String get(String key) {
        return map.get(key);
    }

    @Override
    public void remove(String key) {
        map.remove(key);
    }
}
