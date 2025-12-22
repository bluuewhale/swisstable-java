package io.github.bluuewhale.hashsmith;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class ConcurrentSwissMapConcurrencyTest {
	@Test
	void concurrentPuts_allWritesVisible_andSizeCorrect() throws Exception {
		assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			var m = new ConcurrentSwissMap<Integer, Integer>(16, 16, 0.875d);

			int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
			int keysPerThread = 10_000;
			int expected = threads * keysPerThread;

			ExecutorService pool = Executors.newFixedThreadPool(threads);
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(threads);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			for (int t = 0; t < threads; t++) {
				final int tid = t;
				pool.execute(() -> {
					try {
						start.await();
						int base = tid * keysPerThread;
						for (int i = 0; i < keysPerThread; i++) {
							int k = base + i;
							// Disjoint key ranges: no overwrites, so size must match exactly.
							m.put(k, k);
						}
					} catch (Throwable e) {
						failure.compareAndSet(null, e);
					} finally {
						done.countDown();
					}
				});
			}

			start.countDown();
			assertTrue(done.await(5, TimeUnit.SECONDS), "writer threads did not finish in time");
			pool.shutdownNow();
			pool.awaitTermination(5, TimeUnit.SECONDS);

			Throwable ex = failure.get();
			if (ex != null) fail("unexpected exception during concurrent puts: " + ex, ex);

			// Quiescent state: validate size and content.
			assertEquals(expected, m.size(), "size must equal the number of unique keys inserted");

			for (int k = 0; k < expected; k++) {
				assertTrue(m.containsKey(k), "missing key: " + k);
				assertEquals(k, m.get(k), "incorrect value for key: " + k);
			}

			// Snapshot iterator must have exactly 'expected' unique keys.
			HashSet<Integer> seen = new HashSet<>();
			int count = 0;
			for (var e : m.entrySet()) {
				assertTrue(seen.add(e.getKey()), "duplicate key in snapshot: " + e.getKey());
				count++;
			}
			assertEquals(expected, count, "snapshot entry count must match size");
		});
	}

	@Test
	void concurrentPutAll_allWritesVisible_andSizeCorrect() throws Exception {
		assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			var m = new ConcurrentSwissMap<Integer, Integer>(16, 16, 0.875d);

			int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
			int keysPerThread = 4_000;
			int batchSize = 200;
			int expected = threads * keysPerThread;

			ExecutorService pool = Executors.newFixedThreadPool(threads);
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(threads);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			for (int t = 0; t < threads; t++) {
				final int tid = t;
				pool.execute(() -> {
					try {
						start.await();
						int base = tid * keysPerThread;
						HashMap<Integer, Integer> batch = new HashMap<>(batchSize * 2);
						for (int i = 0; i < keysPerThread; i++) {
							int k = base + i;
							batch.put(k, k);
							if (batch.size() >= batchSize) {
								m.putAll(batch);
								batch.clear();
							}
						}
						if (!batch.isEmpty()) m.putAll(batch);
					} catch (Throwable e) {
						failure.compareAndSet(null, e);
					} finally {
						done.countDown();
					}
				});
			}

			start.countDown();
			assertTrue(done.await(5, TimeUnit.SECONDS), "writer threads did not finish in time");
			pool.shutdownNow();
			pool.awaitTermination(5, TimeUnit.SECONDS);

			Throwable ex = failure.get();
			if (ex != null) fail("unexpected exception during concurrent putAll: " + ex, ex);

			assertEquals(expected, m.size(), "size must equal the number of unique keys inserted");
			for (int k = 0; k < expected; k++) {
				assertTrue(m.containsKey(k), "missing key: " + k);
				assertEquals(k, m.get(k), "incorrect value for key: " + k);
			}
		});
	}

	@Test
	void concurrentOverwritingPuts_sizeEqualsKeySpace_andValuesAreFromAnyWriter() throws Exception {
		assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			// Force resizes; overwrites stress the put path without growing size.
			var m = new ConcurrentSwissMap<Integer, Integer>(16, 16, 0.875d);

			int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
			int keySpace = 10_000;
			int writesPerThread = 100_000;

			ExecutorService pool = Executors.newFixedThreadPool(threads);
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(threads);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			for (int t = 0; t < threads; t++) {
				final int tid = t;
				pool.execute(() -> {
					try {
						start.await();
						ThreadLocalRandom r = ThreadLocalRandom.current();

						// Initialize pass: ensure every key exists at least once.
						// This avoids "missing keys" due to random distribution.
						for (int k = 0; k < keySpace; k++) {
							m.put(k, tid);
						}

						for (int i = 0; i < writesPerThread; i++) {
							int k = r.nextInt(keySpace);
							m.put(k, tid);
						}
					} catch (Throwable e) {
						failure.compareAndSet(null, e);
					} finally {
						done.countDown();
					}
				});
			}

			start.countDown();
			assertTrue(done.await(5, TimeUnit.SECONDS), "writer threads did not finish in time");
			pool.shutdownNow();
			pool.awaitTermination(5, TimeUnit.SECONDS);

			Throwable ex = failure.get();
			if (ex != null) fail("unexpected exception during overwriting puts: " + ex, ex);

			// Quiescent invariants:
			// - All keys must exist.
			// - Size must equal keySpace (only overwrites after initialization).
			// - Each value must be a valid writer id (0..threads-1).
			assertEquals(keySpace, m.size(), "size must stay equal to keySpace under overwriting puts");
			for (int k = 0; k < keySpace; k++) {
				assertTrue(m.containsKey(k), "missing key: " + k);
				Integer v = m.get(k);
				assertNotNull(v, "null value for key: " + k);
				assertTrue(v >= 0 && v < threads, "value must be a writer id; key=" + k + ", v=" + v);
			}
		});
	}

	@Test
	void concurrentPutRemoveOnFixedKeySpace_noExceptions_andEntrySetGetConsistentAfterStop() throws Exception {
		assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			var m = new ConcurrentSwissMap<Integer, Integer>(16, 16, 0.875d);

			int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
			int keySpace = 30_000;

			ExecutorService pool = Executors.newFixedThreadPool(threads);
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(threads);
			AtomicBoolean stop = new AtomicBoolean(false);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			for (int t = 0; t < threads; t++) {
				final int tid = t;
				pool.execute(() -> {
					try {
						start.await();
						ThreadLocalRandom r = ThreadLocalRandom.current();

						while (!stop.get()) {
							int k = r.nextInt(keySpace);
							int op = r.nextInt(100);
							if (op < 60) {
								m.put(k, tid);
							} else {
								m.remove(k);
							}
						}
					} catch (Throwable e) {
						failure.compareAndSet(null, e);
					} finally {
						done.countDown();
					}
				});
			}

			start.countDown();
			TimeUnit.MILLISECONDS.sleep(1000);
			stop.set(true);

			assertTrue(done.await(5, TimeUnit.SECONDS), "worker threads did not finish in time");
			pool.shutdownNow();
			pool.awaitTermination(5, TimeUnit.SECONDS);

			Throwable ex = failure.get();
			if (ex != null) fail("unexpected exception during put/remove: " + ex, ex);

			// After quiescence:
			// - size is within [0, keySpace]
			// - entrySet snapshot has unique keys
			// - for each snapshot entry, containsKey is true and get(key) matches the snapshot value
			int sz = m.size();
			assertTrue(sz >= 0 && sz <= keySpace, "size must be within [0, keySpace], got " + sz);

			HashSet<Integer> seen = new HashSet<>();
			int count = 0;
			for (var e : m.entrySet()) {
				Integer k = e.getKey();
				assertNotNull(k, "null key");
				assertTrue(seen.add(k), "duplicate key in snapshot: " + k);
				assertTrue(m.containsKey(k), "containsKey must be true for snapshot key: " + k);
				assertTrue(Objects.equals(e.getValue(), m.get(k)), "snapshot value must match get(key) for key: " + k);
				count++;
			}
			assertEquals(count, m.size(), "size must match snapshot entry count after quiescence");
		});
	}

	@Test
	void concurrentClearWithWriters_noExceptions_andEmptyAfterFinalClear() throws Exception {
		assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			var m = new ConcurrentSwissMap<Integer, Integer>(16, 16, 0.875d);

			int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
			int writerThreads = Math.max(2, threads - 1);
			int keySpace = 20_000;

			ExecutorService pool = Executors.newFixedThreadPool(writerThreads + 1);
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(writerThreads + 1);
			AtomicBoolean stop = new AtomicBoolean(false);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			// Writers
			for (int t = 0; t < writerThreads; t++) {
				final int tid = t;
				pool.execute(() -> {
					try {
						start.await();
						ThreadLocalRandom r = ThreadLocalRandom.current();
						while (!stop.get()) {
							int k = r.nextInt(keySpace);
							int op = r.nextInt(100);
							if (op < 70) m.put(k, tid);
							else m.remove(k);
						}
					} catch (Throwable e) {
						failure.compareAndSet(null, e);
					} finally {
						done.countDown();
					}
				});
			}

			// Clearer
			pool.execute(() -> {
				try {
					start.await();
					while (!stop.get()) {
						m.clear();
						// Yield a bit to increase interleavings.
						Thread.yield();
					}
				} catch (Throwable e) {
					failure.compareAndSet(null, e);
				} finally {
					done.countDown();
				}
			});

			start.countDown();
			TimeUnit.MILLISECONDS.sleep(1000);
			stop.set(true);

			assertTrue(done.await(5, TimeUnit.SECONDS), "threads did not finish in time");
			pool.shutdownNow();
			pool.awaitTermination(5, TimeUnit.SECONDS);

			Throwable ex = failure.get();
			if (ex != null) fail("unexpected exception during clear+writes: " + ex, ex);

			// Deterministic end state: final clear after quiescence must make it empty.
			m.clear();
			assertEquals(0, m.size(), "map must be empty after final clear");
			assertTrue(m.isEmpty(), "isEmpty must be true after final clear");
			assertEquals(0, m.entrySet().size(), "entrySet must be empty after final clear");
		});
	}

	@Test
	void concurrentIterateAndSizeWhileWriting_noExceptions_andConsistentAfterStop() throws Exception {
		assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			var m = new ConcurrentSwissMap<Integer, Integer>(16, 16, 0.875d);

			int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
			int writerThreads = Math.max(2, threads - 1);
			int keySpace = 30_000;

			ExecutorService pool = Executors.newFixedThreadPool(writerThreads + 1);
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(writerThreads + 1);
			AtomicBoolean stop = new AtomicBoolean(false);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			// Writers
			for (int t = 0; t < writerThreads; t++) {
				final int tid = t;
				pool.execute(() -> {
					try {
						start.await();
						ThreadLocalRandom r = ThreadLocalRandom.current();
						while (!stop.get()) {
							int k = r.nextInt(keySpace);
							int op = r.nextInt(100);
							if (op < 60) m.put(k, (tid << 20) ^ k);
							else m.remove(k);
						}
					} catch (Throwable e) {
						failure.compareAndSet(null, e);
					} finally {
						done.countDown();
					}
				});
			}

			// Reader/iterator thread: repeatedly calls size() and iterates entrySet().
			pool.execute(() -> {
				try {
					start.await();
					while (!stop.get()) {
						// We do not assert consistency during concurrent writes; we only ensure no exceptions.
						m.size();
						for (var e : m.entrySet()) {
							// Touch fields to exercise snapshot entries.
							e.getKey();
							e.getValue();
						}
					}
				} catch (Throwable e) {
					failure.compareAndSet(null, e);
				} finally {
					done.countDown();
				}
			});

			start.countDown();
			TimeUnit.MILLISECONDS.sleep(1000);
			stop.set(true);

			assertTrue(done.await(6, TimeUnit.SECONDS), "threads did not finish in time");
			pool.shutdownNow();
			pool.awaitTermination(5, TimeUnit.SECONDS);

			Throwable ex = failure.get();
			if (ex != null) fail("unexpected exception during iterate+size+writes: " + ex, ex);

			// After quiescence: entrySet snapshot must be self-consistent and match size().
			HashSet<Integer> seen = new HashSet<>();
			int count = 0;
			for (var e : m.entrySet()) {
				Integer k = e.getKey();
				assertNotNull(k, "null key");
				assertTrue(seen.add(k), "duplicate key in snapshot: " + k);
				assertTrue(m.containsKey(k), "containsKey must be true for snapshot key: " + k);
				assertTrue(Objects.equals(e.getValue(), m.get(k)), "snapshot value must match get(key) for key: " + k);
				count++;
			}
			assertEquals(count, m.size(), "size must match snapshot entry count after quiescence");
		});
	}
}


