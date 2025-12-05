package com.donghyungko.swisstable;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.Test;

class SwissMapTest {

	@Test
	void isEmpty() {
		// given
		var m = new SwissMap<String, Integer>();

		// expect
		assertTrue(m.isEmpty());

		// when
		m.put("a", 1);

		// expect
		assertFalse(m.isEmpty());
	}

	@Test
	void containsKey() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		m.put("a", 1);

		// expect
		assertTrue(m.containsKey("a"));
		assertFalse(m.containsKey("b"));
		assertFalse(m.containsKey(null));
	}

	@Test
	void basicCrud() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		var first = m.put("a", 1);
		var replaced = m.put("a", 2); // replace
		var removed = m.remove("a");

		// expect
		assertNull(first);
		assertEquals(1, replaced);
		assertEquals(2, removed);
		assertFalse(m.containsKey("a"));
		assertEquals(0, m.size());
	}

	@Test
	void nullKeyAndValue() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		m.put(null, 10);
		m.put("x", null);
		var replaced = m.put(null, 20); // replace

		// expect
		assertEquals(20, m.get(null));
		assertNull(m.get("x"));
		assertTrue(m.containsKey(null));
		assertTrue(m.containsValue(null));
		assertEquals(10, replaced);
	}

	@Test
	void tombstoneReuse() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		m.put("a", 1);
		m.remove("a"); // create tombstone
		m.put("a", 2); // reuse tombstone

		// expect
		assertEquals(2, m.get("a"));
	}

	@Test
	void rehashOnLoad() {
		// gvivenm
		var m = new SwissMap<Integer, Integer>(4);

		// when: fill over load factor
		for (int i = 0; i < 32; i++) m.put(i, i * 10);

		// expect
		for (int i = 0; i < 32; i++) assertEquals(i * 10, m.get(i));
	}

	@Test
	void entrySetRemoveByValue() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		m.put("a", 1);
		m.put("b", 2);

		// expect
		assertTrue(m.entrySet().remove(Map.entry("a", 1)));
		assertFalse(m.containsKey("a"));
		assertEquals(1, m.size());
		assertFalse(m.entrySet().remove(Map.entry("b", 999))); // value mismatch
		assertTrue(m.containsKey("b"));
	}

	@Test
	void iteratorRemove() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		m.put("a", 1);
		m.put("b", 2);

		// expect
		var it = m.keySet().iterator();
		assertTrue(it.hasNext());

		// when
		it.next();
		it.remove();

		// expect
		assertEquals(1, m.size());
	}

	@Test
	void clearResetsState() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		m.put("a", 1);
		m.clear();

		// expect
		assertEquals(0, m.size());
		assertFalse(m.containsKey("a"));

		// when
		m.put("b", 2);

		// expect
		assertEquals(2, m.get("b"));
	}

	@Test
	void entrySetSetValueReflectsInMap() {
		// given
		var m = new SwissMap<String, Integer>();
		m.put("k", 1);

		// when
		var it = m.entrySet().iterator();
		var e = it.next();
		e.setValue(99);

		// expect
		assertEquals(99, m.get("k"));
	}

	@Test
	void iteratorRemovesAll() {
		// given
		var m = new SwissMap<Integer, Integer>();
		for (int i = 0; i < 10; i++) m.put(i, i);

		// when
		var it = m.entrySet().iterator();
		while (it.hasNext()) {
			it.next();
			it.remove();
		}

		// expect
		assertEquals(0, m.size());
		assertTrue(m.isEmpty());
	}

	@Test
	void highCollisionOnH2() {
		// given: keys with same low 7 bits
		record Fixed(int val) {
			@Override public int hashCode() { return 0x1234_5601; } // low 7 bits == 1
		}
		var m = new SwissMap<Fixed, Integer>();

		// when
		m.put(new Fixed(1), 10);
		m.put(new Fixed(2), 20);
		m.put(new Fixed(3), 30);
		m.remove(new Fixed(2));

		// expect
		assertEquals(10, m.get(new Fixed(1)));
		assertEquals(30, m.get(new Fixed(3)));
		assertNull(m.get(new Fixed(2)));
	}

	@Test
	void putAllBulk() {
		// given
		var m = new SwissMap<Integer, Integer>();
		var src = new java.util.HashMap<Integer, Integer>();
		for (int i = 0; i < 50; i++) src.put(i, i * 2);

		// when
		m.putAll(src);

		// expect
		assertEquals(src.size(), m.size());
		for (int i = 0; i < 50; i++) assertEquals(i * 2, m.get(i));
	}

	@Test
	void valuesContainsAndIteratorRemove() {
		// given
		var m = new SwissMap<String, Integer>();
		m.put("a", 1);
		m.put("b", null);

		// expect contains
		assertTrue(m.values().contains(1));
		assertTrue(m.values().contains(null));

		// when iterator remove via values view
		var it = m.values().iterator();
		assertTrue(it.hasNext());
		it.next();
		it.remove();

		// expect
		assertEquals(1, m.size());
	}

	@Test
	void keySetRemoveAllRetainAll() {
		// given
		var m = new SwissMap<String, Integer>();
		m.put("a", 1); m.put("b", 2); m.put("c", 3);
		var removeSet = java.util.Set.of("a", "x");

		// when
		m.keySet().removeAll(removeSet);

		// expect
		assertFalse(m.containsKey("a"));
		assertEquals(2, m.size());

		// when retain only "b"
		m.keySet().retainAll(java.util.Set.of("b"));

		// expect
		assertTrue(m.containsKey("b"));
		assertEquals(1, m.size());
	}

	@Test
	void iteratorRemoveIllegalState() {
		// given
		var m = new SwissMap<String, Integer>();

		// when
		m.put("a", 1);
		var it = m.entrySet().iterator();

		// expect: remove before next throws
		assertThrows(IllegalStateException.class, it::remove);
	}

	@Test
	void duplicateRemoveIllegalState() {
		// given
		var m = new SwissMap<String, Integer>();
		m.put("a", 1);
		var it = m.entrySet().iterator();
		it.next();
		it.remove();

		// expect: second remove without next throws
		assertThrows(IllegalStateException.class, it::remove);
	}

	@Test
	void largeDeleteAndReinsert() {
		// given
		var m = new SwissMap<Integer, Integer>();
		for (int i = 0; i < 500; i++) m.put(i, i);

		// when: delete most
		for (int i = 0; i < 400; i++) m.remove(i);
		// reinsert
		for (int i = 0; i < 400; i++) m.put(i, i * 2);

		// expect
		assertEquals(500, m.size());
		for (int i = 0; i < 500; i++) {
			int expected = (i < 400) ? i * 2 : i;
			assertEquals(expected, m.get(i));
		}
	}

	@Test
	void entrySetSetValueAcrossAll() {
		// given
		var m = new SwissMap<String, Integer>();
		m.put("a", 1); m.put("b", 2); m.put("c", 3);

		// when: set all values to 100
		for (var e : m.entrySet()) {
			e.setValue(100);
		}

		// expect
		for (var e : m.entrySet()) assertEquals(100, e.getValue());
	}
}
