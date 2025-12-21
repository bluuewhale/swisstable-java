package io.github.bluuewhale.hashsmith;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;

/**
 * A sharded, thread-safe wrapper around {@link SwissMap}.
 *
 * <p>Concurrency model:
 * <ul>
 *   <li><b>Shard selection</b>: choose a shard by the high bits of {@code Hashing.smearedHash(key)}.</li>
 *   <li><b>Reads</b>: {@link StampedLock#tryOptimisticRead()} first, fallback to {@code readLock()}.</li>
 *   <li><b>Writes</b>: {@code writeLock()} per shard, covering put/remove/clear/rehash inside the shard.</li>
 * </ul>
 *
 * <p>Note: The underlying {@link SwissMap} is written for single-threaded use. If an optimistic read
 * overlaps with a writer, the core map may still throw (e.g., NPE) unless the core publish/NULL-safety
 * rules are strengthened. This class is intended to be used together with those follow-up changes.
 */
public final class ConcurrentSwissMap<K, V> extends AbstractMap<K, V> {

	private static final int DEFAULT_INITIAL_CAPACITY = 16;
	private static final double DEFAULT_LOAD_FACTOR = 0.875d;

	private final Shard<K, V>[] shards;
	private final int shardMask;
	private final int shardBits;

	public ConcurrentSwissMap() {
		this(defaultShardCount(), DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
	}

	public ConcurrentSwissMap(int initialCapacity) {
		this(defaultShardCount(), initialCapacity, DEFAULT_LOAD_FACTOR);
	}

	public ConcurrentSwissMap(int initialCapacity, double loadFactor) {
		this(defaultShardCount(), initialCapacity, loadFactor);
	}

	public ConcurrentSwissMap(int shardCount, int initialCapacity, double loadFactor) {
		if (shardCount <= 0) throw new IllegalArgumentException("shardCount must be > 0");
		int sc = Utils.ceilPow2(shardCount);
		this.shardBits = Integer.numberOfTrailingZeros(sc);
		this.shardMask = sc - 1;

		@SuppressWarnings("unchecked")
		Shard<K, V>[] tmp = (Shard<K, V>[]) new Shard[sc];

		int cap = Math.max(DEFAULT_INITIAL_CAPACITY, initialCapacity);
		int perShard = Math.max(1, (cap + sc - 1) / sc);
		for (int i = 0; i < sc; i++) {
			tmp[i] = new Shard<>(new SwissMap<>(perShard, loadFactor));
		}
		this.shards = tmp;
	}

	private static int defaultShardCount() {
		int cores = Runtime.getRuntime().availableProcessors();
		return Utils.ceilPow2(Math.max(1, cores * 2));
	}

	private Shard<K, V> shardFor(Object key) {
		// Keep consistent with the rest of this project: null keys are not supported.
		int h = Hashing.smearedHash(Objects.requireNonNull(key, "Null keys not supported"));
		if (shardBits == 0) return shards[0];
		int idx = (h >>> (Integer.SIZE - shardBits)) & shardMask; // use high bits
		return shards[idx];
	}

	@Override
	public V get(Object key) {
		Shard<K, V> s = shardFor(key);
		long stamp = s.lock.tryOptimisticRead();
		V v = s.map.get(key);
		if (s.lock.validate(stamp)) return v;

        // fallback to read lock
		stamp = s.lock.readLock();
		try {
			return s.map.get(key);
		} finally {
			s.lock.unlockRead(stamp);
		}
	}

	@Override
	public boolean containsKey(Object key) {
		Shard<K, V> s = shardFor(key);
		long stamp = s.lock.tryOptimisticRead();
		boolean ok = s.map.containsKey(key);
		if (s.lock.validate(stamp)) return ok;

        // fallback to read lock
		stamp = s.lock.readLock();
		try {
			return s.map.containsKey(key);
		} finally {
			s.lock.unlockRead(stamp);
		}
	}

	@Override
	public boolean containsValue(Object value) {
		// Read-only scan; lock each shard to avoid concurrent structural changes.
		for (int i = 0; i < shards.length; i++) {
			Shard<K, V> s = shards[i];
			long stamp = s.lock.readLock();
			try {
				if (s.map.containsValue(value)) return true;
			} finally {
				s.lock.unlockRead(stamp);
			}
		}
		return false;
	}

	@Override
	public V put(K key, V value) {
		Shard<K, V> s = shardFor(key);
		long stamp = s.lock.writeLock();
		try {
			return s.map.put(key, value);
		} finally {
			s.lock.unlockWrite(stamp);
		}
	}

	@Override
	public V remove(Object key) {
		Shard<K, V> s = shardFor(key);
		long stamp = s.lock.writeLock();
		try {
			return s.map.remove(key);
		} finally {
			s.lock.unlockWrite(stamp);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		// Phase 1: per-entry locking.
		for (Entry<? extends K, ? extends V> e : m.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void clear() {
		for (int i = 0; i < shards.length; i++) {
			Shard<K, V> s = shards[i];
			long stamp = s.lock.writeLock();
			try {
				s.map.clear();
			} finally {
				s.lock.unlockWrite(stamp);
			}
		}
	}

	@Override
	public int size() {
		int total = 0;
		for (int i = 0; i < shards.length; i++) {
			Shard<K, V> s = shards[i];
			long stamp = s.lock.readLock();
			try {
				total += s.map.size();
			} finally {
				s.lock.unlockRead(stamp);
			}
		}
		return total;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new EntrySetView();
	}

	private final class EntrySetView extends AbstractSet<Entry<K, V>> {
		@Override
		public int size() {
			return ConcurrentSwissMap.this.size();
		}

		@Override
		public void clear() {
			ConcurrentSwissMap.this.clear();
		}

		@Override
		public boolean contains(Object o) {
			if (!(o instanceof Entry<?, ?> e)) return false;
			Object key = e.getKey();
			Object expected = e.getValue();
			Object actual = ConcurrentSwissMap.this.get(key);
			return Objects.equals(actual, expected) && ConcurrentSwissMap.this.containsKey(key);
		}

		@Override
		public boolean remove(Object o) {
			if (!(o instanceof Entry<?, ?> e)) return false;
			Object key = e.getKey();
			Object expected = e.getValue();
			Shard<K, V> s = shardFor(key);
			long stamp = s.lock.writeLock();
			try {
				if (!s.map.containsKey(key)) return false;
				Object actual = s.map.get(key);
				if (!Objects.equals(actual, expected)) return false;
				s.map.remove(key);
				return true;
			} finally {
				s.lock.unlockWrite(stamp);
			}
		}

		@Override
		public Iterator<Entry<K, V>> iterator() {
			// Phase 1: snapshot iterator (weakly consistent). iterator.remove delegates to map.remove.
			ArrayList<Entry<K, V>> snap = snapshotEntries();
			return new SnapshotEntryIterator(snap);
		}
	}

	private ArrayList<Entry<K, V>> snapshotEntries() {
		ArrayList<Entry<K, V>> out = new ArrayList<>();
		for (int i = 0; i < shards.length; i++) {
			Shard<K, V> s = shards[i];
			long stamp = s.lock.readLock();
			try {
				for (Entry<K, V> e : s.map.entrySet()) {
					out.add(new SnapshotEntry(e.getKey(), e.getValue()));
				}
			} finally {
				s.lock.unlockRead(stamp);
			}
		}
		return out;
	}

	private final class SnapshotEntryIterator implements Iterator<Entry<K, V>> {
		private final ArrayList<Entry<K, V>> snap;
		private int idx = 0;
		private K lastKey;
		private boolean canRemove;

		SnapshotEntryIterator(ArrayList<Entry<K, V>> snap) {
			this.snap = snap;
		}

		@Override
		public boolean hasNext() {
			return idx < snap.size();
		}

		@Override
		public Entry<K, V> next() {
			Entry<K, V> e = snap.get(idx++);
			lastKey = e.getKey();
			canRemove = true;
			return e;
		}

		@Override
		public void remove() {
			if (!canRemove) throw new IllegalStateException();
			ConcurrentSwissMap.this.remove(lastKey);
			canRemove = false;
		}
	}

	private final class SnapshotEntry implements Entry<K, V> {
		private final K key;
		private V value;

		SnapshotEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V newValue) {
			V old = ConcurrentSwissMap.this.put(key, newValue);
			this.value = newValue;
			return old;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Entry<?, ?> e)) return false;
			return Objects.equals(key, e.getKey()) && Objects.equals(value, e.getValue());
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(key) ^ Objects.hashCode(value);
		}
	}

	private static final class Shard<K, V> {
		final StampedLock lock = new StampedLock();
		final SwissMap<K, V> map;
		Shard(SwissMap<K, V> map) { this.map = map; }
	}
}


