package io.github.bluuewhale.hashsmith;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SwissMapRehashResizeTest {

	@Test
	void tombstoneRehashDoesNotResize() {
		var m = new SwissMap<Integer, Integer>(64);
		int initialCap = m.capacity;

		// Insert a small fixed set; (size + tombstones) stays constant during deletes,
		// so we do NOT exceed maxLoad. Eventually tombstones > size/2 triggers rehash.
		for (int i = 0; i < 16; i++) m.put(i, i * 10);

		for (int i = 0; i < 9; i++) {
			assertEquals(i * 10, m.remove(i));
		}

		// Rehash may have happened due to tombstones, but capacity must not grow.
		assertEquals(initialCap, m.capacity);

		for (int i = 0; i < 9; i++) assertNull(m.get(i));
		for (int i = 9; i < 16; i++) assertEquals(i * 10, m.get(i));
		assertEquals(7, m.size());
	}

	@Test
	void overMaxLoadRehashDoesResize() {
		var m = new SwissMap<Integer, Integer>(16);
		int cap0 = m.capacity;
		int maxLoad0 = m.maxLoad;

		// Fill up to maxLoad; resize happens on the *next* put (maybeResize runs before insert).
		for (int i = 0; i < maxLoad0; i++) m.put(i, i);
		m.put(maxLoad0, maxLoad0);

		assertTrue(m.capacity >= cap0 * 2, "capacity should grow when exceeding maxLoad");
		assertEquals(maxLoad0 + 1, m.size());
		for (int i = 0; i <= maxLoad0; i++) assertEquals(i, m.get(i));
	}
}


