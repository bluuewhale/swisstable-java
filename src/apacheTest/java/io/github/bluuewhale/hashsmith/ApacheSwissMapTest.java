package io.github.bluuewhale.hashsmith;

import org.apache.commons.collections4.map.AbstractMapTest;

import java.util.Map;

final class ApacheSwissMapTest<K, V> extends AbstractMapTest<Map<K, V>, K, V> {
    @Override public boolean isAllowNullKey() {
        return false;
    }
    @Override public boolean isAllowNullValueGet() {
        return true;
    }
    @Override public boolean isAllowNullValuePut() {
        return true;
    }
    @Override public Map<K, V> makeObject() {
        return new SwissMap<>();
    }
}
