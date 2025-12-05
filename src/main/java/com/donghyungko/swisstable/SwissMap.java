package com.donghyungko.swisstable;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Skeleton for a SwissTable-inspired Map implementation.
 * All methods are not implemented yet; logic will be filled later.
 */
public class SwissMap<K, V> extends AbstractMap<K, V> {

	/* Control byte values */
	private static final byte EMPTY = (byte) 0x80;    // empty slot
	private static final byte DELETED = (byte) 0xFE;  // tombstone
	// private static final byte SENTINEL = (byte) 0xFF; // optional padding for control array (unused)

	/* Hash split masks: high bits choose group, low 7 bits stored in control byte */
	private static final int H1_MASK = 0xFFFFFF80;
	private static final int H2_MASK = 0x0000007F;

	/* Group sizing: 1 << shift equals slots per group; align with Abseil width(kWidth)*/
	private static final int DEFAULT_SHIFT = 4; // default 16 slots (128-bit SIMD)

	/* Storage and state */
	private byte[] ctrl;     // control bytes (EMPTY/DELETED/lo-hash)
	private Object[] keys;   // key storage
	private Object[] vals;   // value storage
	private int size;        // live entries
	private int tombstones;  // deleted slots
	private int capacity;    // total slots (length of ctrl/keys/vals)
	private int shift = DEFAULT_SHIFT; // log2 of slots per group

	@Override
	public int size() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean containsKey(Object key) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public V get(Object key) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public V put(K key, V value) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException("Not implemented yet");
	}
}
