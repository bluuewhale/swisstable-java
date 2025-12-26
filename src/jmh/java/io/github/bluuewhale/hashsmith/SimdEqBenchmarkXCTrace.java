package io.github.bluuewhale.hashsmith;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@Fork(
    value = 1,
    jvmArgsAppend = {
        "--add-modules=jdk.incubator.vector",
        "--enable-preview",
        "-Xms2g",
        "-Xmx2g",
        "-XX:+AlwaysPreTouch",
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+DebugNonSafepoints"
    }
)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SimdEqBenchmarkXCTrace {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int LOOP = 4096;

    @State(Scope.Thread)
    public static class S {
        @Param({ "0" })
        public int base;

        @Param({ "0" })
        public int valueParam;

        public byte[] array;
        public byte value;
        public ByteVector loaded;

        @Setup(Level.Trial)
        public void setup() {
            array = new byte[4096];
            for (int i = 0; i < array.length; i++) {
                array[i] = (byte) (i * 31 + 7);
            }
            value = (byte) valueParam;
            loaded = ByteVector.fromArray(SPECIES, array, base);
        }
    }

    @State(Scope.Thread)
    public static class SwissGetHitState {
        /**
         * Number of distinct hit keys.
         * Note: rounded up to the next power-of-two in {@link #setup()} for cheap indexing.
         */
        @Param({ "65536" })
        public int size;

        int mask;
        Integer[] keys;
        SwissMap<Integer, Integer> swiss;
        SwissSimdMap<Integer, Integer> swissSimd;

        @Setup(Level.Trial)
        public void setup() {
            int n = Utils.ceilPow2(size);
            this.mask = n - 1;

            this.keys = new Integer[n];
            this.swiss = new SwissMap<>(n * 2);
            this.swissSimd = new SwissSimdMap<>(n * 2);

            for (int i = 0; i < n; i++) {
                keys[i] = i;
                swiss.put(i, i);
                swissSimd.put(i, i);
            }
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private static long eq_toLong(ByteVector v, byte value) {
        return v.eq(value).toLong();
    }

    @Benchmark
    @OperationsPerInvocation(LOOP)
    public long eq_toLong_loop(S s) {
        long acc = 0;
        for (int i = 0; i < LOOP; i++) {
            acc ^= eq_toLong(s.loaded, s.value);
        }
        return acc;
    }

    @Benchmark
    public void pipeline_loop(SimdEqBenchmark.S s, Blackhole bh) {
        long acc = 0;
        byte[] array = s.array;
        byte value = s.value;
        int base = s.base;

        for (int i = 0; i < LOOP; i++) {
            ByteVector loaded = ByteVector.fromArray(SPECIES, array, base);
            acc ^= loaded.eq(value).toLong();
        }
        bh.consume(acc);
    }

    /**
     * SwissTable-style "get hit" microbench for xctrace/hsdis:
     * repeatedly hits existing keys, exercising {@link AbstractArrayMap#get(Object)} -> findIndex.
     */
    @Benchmark
    @OperationsPerInvocation(LOOP)
    public int swiss_getHit_loop(SwissGetHitState s) {
        int acc = 0;
        int idx = 0;
        Integer[] keys = s.keys;
        int mask = s.mask;
        SwissMap<Integer, Integer> map = s.swiss;
        for (int i = 0; i < LOOP; i++) {
            int key = keys[idx];
            int v = map.get(key);
            acc ^= v; // NPE if miss: intentional invariant check
            idx = (idx + 1) & mask;
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(LOOP)
    public int swissSimd_getHit_loop(SwissGetHitState s) {
        int acc = 0;
        int idx = 0;
        Integer[] keys = s.keys;
        SwissSimdMap<Integer, Integer> map = s.swissSimd;
        int mask = s.mask;
        for (int i = 0; i < LOOP; i++) {
            Integer key = keys[idx];
            int v = map.get(key);
            acc ^= v; // NPE if miss: intentional invariant check
            idx = (idx + 1) & mask;
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(LOOP)
    public int swissSimd_findIndexHit_loop(SwissGetHitState s) {
        int acc = 0;
        Integer[] keys = s.keys;
        SwissSimdMap<Integer, Integer> map = s.swissSimd;
        for (int i = 0; i < LOOP; i += 1) {
            int v = map.findIndex(keys[i]);
            acc ^= v;
        }
        return acc;
    }

    @Benchmark
    @OperationsPerInvocation(LOOP)
    public int swiss_findIndexHit_loop(SwissGetHitState s) {
        int acc = 0;
        Integer[] keys = s.keys;
        SwissMap<Integer, Integer> map = s.swiss;
        for (int i = 0; i < LOOP; i += 1) {
            int v = map.findIndex(keys[i]);
            acc ^= v;
        }
        return acc;
    }
}

