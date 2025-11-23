package org.FantaDB.LibFanta.KV.Engine;

import org.FantaDB.LibFanta.KV.Storage.AppendOnlyStorage;

public class PersistentKVEngine implements KVEngine {
    private final AppendOnlyStorage appendOnlyStorage;

    public PersistentKVEngine(AppendOnlyStorage appendOnlyStorage) {
        this.appendOnlyStorage = appendOnlyStorage;
    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public String get(String key) {
        return "";
    }

    @Override
    public void remove(String key) {

    }
}
