package org.FantaDB.LibFanta.KV.Engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryKVEngineTest {

    private InMemoryKVEngine engine;

    @BeforeEach
    void setUp() {
        engine = new InMemoryKVEngine();
    }

    @Test
    void testPutAndGet() {
        engine.put("key1", "value1");
        engine.put("key2", "value2");

        assertEquals("value1", engine.get("key1"));
        assertEquals("value2", engine.get("key2"));
    }

    @Test
    void testOverwriteValue() {
        engine.put("key", "v1");
        assertEquals("v1", engine.get("key"));

        engine.put("key", "v2");
        assertEquals("v2", engine.get("key"));
    }

    @Test
    void testRemove() {
        engine.put("key", "value");
        assertEquals("value", engine.get("key"));

        engine.remove("key");
        assertNull(engine.get("key"));
    }

    @Test
    void testGetNonExistentKey() {
        assertNull(engine.get("missing"));
    }

    @Test
    void testRemoveNonExistentKey() {
        engine.remove("missing"); // should not throw
        assertNull(engine.get("missing"));
    }
}
