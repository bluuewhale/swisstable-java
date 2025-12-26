package io.github.bluuewhale.hashsmith;

/**
 * A tiny harness to heat up {@code SwissMap.findIndexHashed} via the hash-injected fast path
 * {@code SwissMap.get(key, smearedHash)} so that assembly output is focused on the real lookup hot-path.
 *
 * Run via:
 *   ./gradlew jitAsm
 *
 * Note: {@code -XX:+PrintAssembly} requires an hsdis disassembler library to be present for your JDK.
 * See docs/JIT-ASM.md.
 */
public final class SwissMapAsmProbe {
	private static final int N_KEYS = 1 << 14;
	private static final int OPS = 5_000_000;

	public static void main(String[] args) {
		SwissMap<String, Integer> m = new SwissMap<>(N_KEYS);

		// Precompute keys + smeared hashes so the hot loop does not allocate or re-hash.
		String[] keys = new String[N_KEYS];
		int[] hashes = new int[N_KEYS];
		for (int i = 0; i < N_KEYS; i++) {
			String k = "k" + i;
			keys[i] = k;
			hashes[i] = Hashing.smearedHash(k);
		}

		// Pre-fill so lookups don't early-out on size == 0 and we hit the probe loop.
		for (int i = 0; i < N_KEYS; i++) {
			m.put(keys[i], i, hashes[i]);
		}

		// Warmup + measurement-ish loop. We want a very hot call-site so C2 compiles it.
		long sum = 0;
		final int mask = N_KEYS - 1;
		for (int i = 0; i < OPS; i++) {
			// All keys are present (pre-filled above), so get() should not return null.
			int idx = i & mask;
			sum += m.get(keys[idx], hashes[idx]);
		}

		// Prevent dead-code elimination of the loop.
		System.out.println("sum=" + sum);
	}
}


