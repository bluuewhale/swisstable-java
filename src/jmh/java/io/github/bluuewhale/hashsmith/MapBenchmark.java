package io.github.bluuewhale.hashsmith;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(2)
@Warmup(iterations = 3)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MapBenchmark {

	@State(Scope.Benchmark)
	public static class ReadState {
		@Param({ "10000", "100000", "200000", "400000" })
		int size;

		SwissMap<Integer, Integer> swiss;
		RobinHoodMap<Integer, Integer> robin;
		UnifiedMap<Integer, Integer> unified;
		HashMap<Integer, Integer> jdk;
		int[] keys;
		int[] misses;
		Random rnd;
		int nextKeyIndex;
		int nextMissIndex;
		Set<Integer> keySet;

		@Setup(Level.Trial)
		public void setup() {
			rnd = new Random(123);
			keys = new int[size];
			misses = new int[size];
			keySet = new java.util.HashSet<>(size * 2);
			for (int i = 0; i < size; i++) {
				int k = rnd.nextInt();
				keys[i] = k;
				keySet.add(k);
			}
			for (int i = 0; i < size; i++) {
				int miss;
				do { miss = rnd.nextInt(); } while (keySet.contains(miss));
				misses[i] = miss;
			}
			nextKeyIndex = 0;
			nextMissIndex = 0;
			swiss = new SwissMap<>();
			robin = new RobinHoodMap<>();
			unified = new UnifiedMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				swiss.put(keys[i], i);
				robin.put(keys[i], i);
				unified.put(keys[i], i);
				jdk.put(keys[i], i);
			}
		}

		int nextHitKey() {
			int k = keys[nextKeyIndex];
			nextKeyIndex = (nextKeyIndex + 1) % keys.length;
			return k;
		}
		int nextMissingKey() {
			int k = misses[nextMissIndex];
			nextMissIndex = (nextMissIndex + 1) % misses.length;
			return k;
		}
	}

	@State(Scope.Thread)
	public static class MutateState {
        @Param({ "10000", "100000", "200000", "400000" })
		int size;

		int[] keys;
		int[] misses;
		Random rnd;
		int putValue;
		int existingIndex;
		int missingIndex;
		SwissMap<Integer, Integer> swiss;
		RobinHoodMap<Integer, Integer> robin;
		UnifiedMap<Integer, Integer> unified;
		HashMap<Integer, Integer> jdk;

		@Setup(Level.Trial)
		public void initKeys() {
			rnd = new Random(456);
			keys = IntStream.range(0, size).map(i -> rnd.nextInt()).toArray();
			misses = IntStream.range(0, size).map(i -> rnd.nextInt()).toArray();
		}

		@Setup(Level.Iteration)
		public void resetMaps() {
			swiss = new SwissMap<>();
			robin = new RobinHoodMap<>();
			unified = new UnifiedMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				swiss.put(keys[i], i);
				robin.put(keys[i], i);
				unified.put(keys[i], i);
				jdk.put(keys[i], i);
			}
			putValue = 0;
			existingIndex = 0;
			missingIndex = 0;
		}

		int nextHitKey() {
			int k = keys[existingIndex];
			existingIndex = (existingIndex + 1) % keys.length;
			return k;
		}
		int nextMissingKey() {
			int k = misses[missingIndex];
			missingIndex = (missingIndex + 1) % misses.length;
			return k;
		}
		int nextValue() { return ++putValue; }
	}

	@State(Scope.Thread)
	public static class RemoveState {
		@Param({ "100", "1000", "10000" })
		int size;

		SwissMap<Integer, Integer> swiss;
		RobinHoodMap<Integer, Integer> robin;
		UnifiedMap<Integer, Integer> unified;
		HashMap<Integer, Integer> jdk;
		int[] keys;
		int[] misses;
		Random rnd;

		@Setup(Level.Trial)
		public void initData() {
			rnd = new Random(789);
			keys = new int[size];
			misses = new int[size];
			var keySet = new java.util.HashSet<>(size * 2);
			for (int i = 0; i < size; i++) {
				int k = rnd.nextInt();
				keys[i] = k;
				keySet.add(k);
			}
			for (int i = 0; i < size; i++) {
				int miss;
				do { miss = rnd.nextInt(); } while (keySet.contains(miss));
				misses[i] = miss;
			}
		}

		@Setup(Level.Invocation)
		public void resetMaps() {
			swiss = new SwissMap<>();
			robin = new RobinHoodMap<>();
			unified = new UnifiedMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				swiss.put(keys[i], i);
				robin.put(keys[i], i);
				unified.put(keys[i], i);
				jdk.put(keys[i], i);
			}
		}

		int hitKey() { return keys[rnd.nextInt(keys.length)]; }
		int missKey() { return misses[rnd.nextInt(misses.length)]; }
	}

	// ------- get hit/miss -------
//	@Benchmark
	public int swissGetHit(ReadState s) {
		return s.swiss.get(s.nextHitKey());
	}

//	@Benchmark
	public int robinGetHit(ReadState s) {
		return s.robin.get(s.nextHitKey());
	}

//	 @Benchmark
	public int unifiedGetHit(ReadState s) {
		return s.unified.get(s.nextHitKey());
	}

//	@Benchmark
	public int jdkGetHit(ReadState s) {
		return s.jdk.get(s.nextHitKey());
	}

//	@Benchmark
	public int swissGetMiss(ReadState s) {
		Integer v = s.swiss.get(s.nextMissingKey());
		return v == null ? -1 : v;
	}

//	@Benchmark
	public int robinGetMiss(ReadState s) {
		Integer v = s.robin.get(s.nextMissingKey());
		return v == null ? -1 : v;
	}

//	 @Benchmark
	public int unifiedGetMiss(ReadState s) {
		Integer v = s.unified.get(s.nextMissingKey());
		return v == null ? -1 : v;
	}

//	@Benchmark
	public int jdkGetMiss(ReadState s) {
		Integer v = s.jdk.get(s.nextMissingKey());
		return v == null ? -1 : v;
	}

	// ------- iterate -------
//	@Benchmark
	public long swissIterate(ReadState s) {
		long sum = 0;
		for (var e : s.swiss.entrySet()) sum += e.getValue();
		return sum;
	}

//	@Benchmark
	public long robinIterate(ReadState s) {
		long sum = 0;
		for (var e : s.robin.entrySet()) sum += e.getValue();
		return sum;
	}

	// @Benchmark
	public long unifiedIterate(ReadState s) {
		long sum = 0;
		for (var e : s.unified.entrySet()) sum += e.getValue();
		return sum;
	}

//	@Benchmark
	public long jdkIterate(ReadState s) {
		long sum = 0;
		for (var e : s.jdk.entrySet()) sum += e.getValue();
		return sum;
	}

	// ------- mutating: put hit/miss -------
//	@Benchmark
	public int swissPutHit(MutateState s) {
		int k = s.nextHitKey();
		return s.swiss.put(k, s.nextValue());
	}

	//  @Benchmark
	public int robinPutHit(MutateState s) {
		int k = s.nextHitKey();
		Integer prev = s.robin.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int unifiedPutHit(MutateState s) {
		int k = s.nextHitKey();
		Integer prev = s.unified.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

//	@Benchmark
	public int jdkPutHit(MutateState s) {
		int k = s.nextHitKey();
		return s.jdk.put(k, s.nextValue());
	}

//	@Benchmark
	public int swissPutMiss(MutateState s) {
		int k = s.nextMissingKey();
		Integer prev = s.swiss.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

	//  @Benchmark
	public int robinPutMiss(MutateState s) {
		int k = s.nextMissingKey();
		Integer prev = s.robin.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int unifiedPutMiss(MutateState s) {
		int k = s.nextMissingKey();
		Integer prev = s.unified.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

//	@Benchmark
	public int jdkPutMiss(MutateState s) {
		int k = s.nextMissingKey();
		Integer prev = s.jdk.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

	// ------- remove hit/miss -------
	// @Benchmark
	public int swissRemoveHit(RemoveState s) {
		Integer prev = s.swiss.remove(s.hitKey());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int robinRemoveHit(RemoveState s) {
		Integer prev = s.robin.remove(s.hitKey());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int unifiedRemoveHit(RemoveState s) {
		Integer prev = s.unified.remove(s.hitKey());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int jdkRemoveHit(RemoveState s) {
		Integer prev = s.jdk.remove(s.hitKey());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int swissRemoveMiss(RemoveState s) {
		Integer prev = s.swiss.remove(s.missKey());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int robinRemoveMiss(RemoveState s) {
		Integer prev = s.robin.remove(s.missKey());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int unifiedRemoveMiss(RemoveState s) {
		Integer prev = s.unified.remove(s.missKey());
		return prev == null ? -1 : prev;
	}

	// @Benchmark
	public int jdkRemoveMiss(RemoveState s) {
		Integer prev = s.jdk.remove(s.missKey());
		return prev == null ? -1 : prev;
	}
}

