package com.donghyungko.swisstable;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
public class SwissMapBenchmark {

	@State(Scope.Benchmark)
	public static class ReadState {
		@Param({ "1000", "10000" })
		int size;

		SwissMap<Integer, Integer> swiss;
		HashMap<Integer, Integer> jdk;
		int[] keys;
		int[] misses;
		Random rnd;
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
			swiss = new SwissMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				swiss.put(keys[i], i);
				jdk.put(keys[i], i);
			}
		}

		int nextKey() { return keys[rnd.nextInt(keys.length)]; }
		int nextMiss() { return misses[rnd.nextInt(misses.length)]; }
	}

	@State(Scope.Thread)
	public static class MutateState {
		@Param({ "1000", "10000" })
		int size;

		int[] keys;
		int[] misses;
		int putValue;
		SwissMap<Integer, Integer> swiss;
		HashMap<Integer, Integer> jdk;

		@Setup(Level.Trial)
		public void initKeys() {
			var rnd = new Random(456);
			keys = IntStream.range(0, size).map(i -> rnd.nextInt()).toArray();
			misses = IntStream.range(0, size).map(i -> rnd.nextInt()).toArray();
		}

		@Setup(Level.Iteration)
		public void resetMaps() {
			swiss = new SwissMap<>();
			jdk = new HashMap<>();
			for (int i = 0; i < size; i++) {
				swiss.put(keys[i], i);
				jdk.put(keys[i], i);
			}
			putValue = 0;
		}

		int existingKey(int i) { return keys[i % keys.length]; }
		int missingKey(int i) { return misses[i % misses.length]; }
		int nextValue() { return ++putValue; }
	}

	// ------- get hit/miss -------
	@Benchmark
	public int swissGetHit(ReadState s) {
		return s.swiss.get(s.nextKey());
	}

	@Benchmark
	public int jdkGetHit(ReadState s) {
		return s.jdk.get(s.nextKey());
	}

	@Benchmark
	public int swissGetMiss(ReadState s) {
		Integer v = s.swiss.get(s.nextMiss());
		return v == null ? -1 : v;
	}

	@Benchmark
	public int jdkGetMiss(ReadState s) {
		Integer v = s.jdk.get(s.nextMiss());
		return v == null ? -1 : v;
	}

	// ------- iterate -------
	@Benchmark
	public long swissIterate(ReadState s) {
		long sum = 0;
		for (var e : s.swiss.entrySet()) sum += e.getValue();
		return sum;
	}

	@Benchmark
	public long jdkIterate(ReadState s) {
		long sum = 0;
		for (var e : s.jdk.entrySet()) sum += e.getValue();
		return sum;
	}

	// ------- mutating: put hit/miss -------
	@Benchmark
	public int swissPutHit(MutateState s) {
		int k = s.existingKey(0);
		return s.swiss.put(k, s.nextValue());
	}

	@Benchmark
	public int jdkPutHit(MutateState s) {
		int k = s.existingKey(0);
		return s.jdk.put(k, s.nextValue());
	}

	@Benchmark
	public int swissPutMiss(MutateState s) {
		int k = s.missingKey(s.putValue);
		Integer prev = s.swiss.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

	@Benchmark
	public int jdkPutMiss(MutateState s) {
		int k = s.missingKey(s.putValue);
		Integer prev = s.jdk.put(k, s.nextValue());
		return prev == null ? -1 : prev;
	}

	// ------- mutating: remove -------
	@Benchmark
	public int swissRemoveHit(MutateState s) {
		int k = s.existingKey(1);
		return s.swiss.remove(k);
	}

	@Benchmark
	public int jdkRemoveHit(MutateState s) {
		int k = s.existingKey(1);
		return s.jdk.remove(k);
	}
}
