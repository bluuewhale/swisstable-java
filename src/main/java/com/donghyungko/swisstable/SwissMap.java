package com.donghyungko.swisstable;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.Objects;

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
	private static final int DEFAULT_GROUP_SIZE = 1 << DEFAULT_SHIFT;

	/* Storage and state */
	private byte[] ctrl;     // control bytes (EMPTY/DELETED/H2 fingerprint)
	private Object[] keys;   // key storage
	private Object[] vals;   // value storage
	private int size;        // live entries
	private int tombstones;  // deleted slots
	private int capacity;    // total slots (length of ctrl/keys/vals)
	private int shift = DEFAULT_SHIFT; // log2 of slots per group
	private int maxLoad;     // threshold to trigger rehash/resize

	public SwissMap() {
		this(16);
	}

	public SwissMap(int initialCapacity) {
		init(initialCapacity);
	}

	private void init(int desiredCapacity) {
		int nGroups = Math.max(1, (desiredCapacity + DEFAULT_GROUP_SIZE - 1) / DEFAULT_GROUP_SIZE);
		this.capacity = nGroups * DEFAULT_GROUP_SIZE;

		this.ctrl = new byte[capacity];
		Arrays.fill(this.ctrl, EMPTY);
		this.keys = new Object[capacity];
		this.vals = new Object[capacity];
		this.size = 0;
		this.tombstones = 0;
		this.maxLoad = calcMaxLoad(this.capacity);
	}

	/* Hash split helpers */
	private int h1(int hash) {
		return (hash & H1_MASK) >>> 7;
	}

	private byte h2(int hash) {
		return (byte) (hash & H2_MASK);
	}

	private int hash(Object key) {
		return (key == null) ? 0 : key.hashCode();
	}

	/* Capacity/load helpers */
	private int calcMaxLoad(int cap) {
		// similar to Abseil's 7/8 load factor reserve
		return (cap * 7) >>> 3;
	}

	private int numGroups() {
		return capacity / DEFAULT_GROUP_SIZE;
	}

	private boolean shouldRehash() {
		// trigger when over load or too many tombstones
		return (size + tombstones) >= maxLoad || tombstones > (size >>> 1);
	}

	/* Control byte inspectors */
	private boolean isEmpty(byte c) { return c == EMPTY; }
	private boolean isDeleted(byte c) { return c == DELETED; }
	private boolean isFull(byte c) { return c >= 0 && c <= H2_MASK; } // H2 in [0,127]

	/* Resize/rehash skeletons (implementation to be filled later) */
	private void maybeResize() {
		if (!shouldRehash()) return;
		int newCap = Math.max(capacity * 2, DEFAULT_GROUP_SIZE);
		rehash(newCap);
	}

	private void rehash(int newCapacity) {
		byte[] oldCtrl = this.ctrl;
		Object[] oldKeys = this.keys;
		Object[] oldVals = this.vals;

		this.capacity = Math.max(newCapacity, DEFAULT_GROUP_SIZE);
		this.ctrl = new byte[this.capacity];
		Arrays.fill(this.ctrl, EMPTY);
		this.keys = new Object[this.capacity];
		this.vals = new Object[this.capacity];
		this.size = 0;
		this.tombstones = 0;
		this.maxLoad = calcMaxLoad(this.capacity);

		if (oldCtrl == null) return;

		for (int i = 0; i < oldCtrl.length; i++) {
			byte c = oldCtrl[i];
			if (!isFull(c)) continue;
			@SuppressWarnings("unchecked")
			K k = (K) oldKeys[i];
			@SuppressWarnings("unchecked")
			V v = (V) oldVals[i];
			int h = (k == null) ? 0 : k.hashCode();
			insertFresh(k, v, h1(h), h2(h));
		}
	}

	/* fresh-table insertion used only during rehash */
	private void insertFresh(K key, V value, int h1, byte h2) {
		int nGroups = numGroups();
		if (nGroups == 0) { throw new IllegalStateException("No groups allocated"); }
		int g = h1 % nGroups;
		for (;;) {
			int base = g * DEFAULT_GROUP_SIZE;
			for (int j = 0; j < DEFAULT_GROUP_SIZE; j++) {
				int idx = base + j;
				if (isEmpty(ctrl[idx])) {
					ctrl[idx] = h2;
					keys[idx] = key;
					vals[idx] = value;
					size++;
					return;
				}
			}
			g++;
			if (g == nGroups) g = 0;
		}
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		return findIndex(key) >= 0;
	}

	@Override
	public boolean containsValue(Object value) {
		// linear scan; acceptable for now
		for (int i = 0; i < ctrl.length; i++) {
			if (isFull(ctrl[i])) {
				if (Objects.equals(vals[i], value)) return true;
			}
		}
		return false;
	}

	@Override
	public V get(Object key) {
		int idx = findIndex(key);
		return (idx >= 0) ? castValue(vals[idx]) : null;
	}

	@Override
	public V put(K key, V value) {
		maybeResize();
		int h = hash(key);
		int h1 = h1(h);
		byte h2 = h2(h);
		int nGroups = numGroups();
		int firstTombstone = -1;
		int g = h1 % nGroups;
		for (;;) {
			int base = g * DEFAULT_GROUP_SIZE;
			for (int j = 0; j < DEFAULT_GROUP_SIZE; j++) {
				int idx = base + j;
				byte c = ctrl[idx];
				if (isEmpty(c)) {
					// reuse earliest tombstone if we saw one; ensures we still
					// scan for an existing key before deciding insertion slot
					int target = (firstTombstone >= 0) ? firstTombstone : idx;
					return insertAt(target, key, value, h2);
				}
				if (isDeleted(c) && firstTombstone < 0) {
					firstTombstone = idx;
					continue;
				}
				if (isFull(c) && c == h2 && Objects.equals(keys[idx], key)) {
					@SuppressWarnings("unchecked")
					V old = (V) vals[idx];
					vals[idx] = value;
					return old;
				}
			}
			g++;
			if (g == nGroups) g = 0;
		}
	}

	@Override
	public V remove(Object key) {
		int idx = findIndex(key);
		if (idx < 0) return null;
		@SuppressWarnings("unchecked")
		V old = (V) vals[idx];
		ctrl[idx] = DELETED;
		keys[idx] = null;
		vals[idx] = null;
		size--;
		tombstones++;
		return old;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (Entry<? extends K, ? extends V> e : m.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void clear() {
		Arrays.fill(ctrl, EMPTY);
		Arrays.fill(keys, null);
		Arrays.fill(vals, null);
		size = 0;
		tombstones = 0;
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

	/* lookup utilities */
	private int findIndex(Object key) {
		if (size == 0) return -1;
		int h = hash(key);
		int h1 = h1(h);
		byte h2 = h2(h);
		int nGroups = numGroups();
		int g = h1 % nGroups;
		for (;;) {
			int base = g * DEFAULT_GROUP_SIZE;
			for (int j = 0; j < DEFAULT_GROUP_SIZE; j++) {
				int idx = base + j;
				byte c = ctrl[idx];
				if (isEmpty(c)) return -1;
				if (isFull(c) && c == h2 && Objects.equals(keys[idx], key)) {
					return idx;
				}
			}
			g++;
			if (g == nGroups) g = 0;
		}
	}

	private V insertAt(int idx, K key, V value, byte h2) {
		if (isDeleted(ctrl[idx])) tombstones--;
		ctrl[idx] = h2;
		keys[idx] = key;
		vals[idx] = value;
		size++;
		return null;
	}

	@SuppressWarnings("unchecked")
	private V castValue(Object v) {
		return (V) v;
	}
}
