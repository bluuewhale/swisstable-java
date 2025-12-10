package io.github.bluuewhale.hashsmith;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class SwissSetTest {

	@Test
	void basicAddRemove() {
		var s = new SwissSet<String>();

		assertTrue(s.add("a"));
		assertFalse(s.add("a"));
		assertTrue(s.contains("a"));
		assertEquals(1, s.size());

		assertTrue(s.remove("a"));
		assertFalse(s.contains("a"));
		assertEquals(0, s.size());
		assertFalse(s.remove("a"));
	}

	@Test
	void nullSupported() {
		var s = new SwissSet<String>();

		assertTrue(s.add(null));
		assertTrue(s.contains(null));
		assertTrue(s.remove(null));
		assertFalse(s.contains(null));

		assertTrue(s.add(null));
		assertTrue(s.add("x"));
		assertTrue(s.contains("x"));
		assertEquals(2, s.size());
	}

	@Test
	void tombstoneReuse() {
		var s = new SwissSet<String>();
		assertTrue(s.add("a"));
		assertTrue(s.remove("a"));
		assertTrue(s.add("a"));
		assertEquals(1, s.size());
		assertTrue(s.contains("a"));
	}

	@Test
	void rehashOnLoad() {
		var s = new SwissSet<Integer>(4);
		for (int i = 0; i < 64; i++) assertTrue(s.add(i));
		for (int i = 0; i < 64; i++) assertTrue(s.contains(i));
		assertEquals(64, s.size());
	}

	@Test
	void iteratorRemove() {
		var s = new SwissSet<String>();
		s.add("a");
		s.add("b");

		Iterator<String> it = s.iterator();
		assertTrue(it.hasNext());
		it.next();
		it.remove();

		assertEquals(1, s.size());
		assertTrue(it.hasNext()); // should still have next element
	}

	@Test
	void iteratorRemoveIllegalState() {
		var s = new SwissSet<String>();
		s.add("a");
		Iterator<String> it = s.iterator();
		assertThrows(IllegalStateException.class, it::remove);
	}

	@Test
	void duplicateRemoveIllegalState() {
		var s = new SwissSet<String>();
		s.add("a");
		Iterator<String> it = s.iterator();
		it.next();
		it.remove();
		assertThrows(IllegalStateException.class, it::remove);
	}

	@Test
	void retainAndRemoveAll() {
		var s = new SwissSet<String>();
		s.addAll(List.of("a", "b", "c"));
		assertTrue(s.removeAll(Set.of("a", "x")));
		assertEquals(Set.of("b", "c"), s);
		assertTrue(s.retainAll(Set.of("b")));
		assertEquals(Set.of("b"), s);
	}

	@Test
	void highCollision() {
		record Fixed(int val) {
			@Override public int hashCode() { return 0x1234_5601; }
		}
		var s = new SwissSet<Fixed>();
		assertTrue(s.add(new Fixed(1)));
		assertTrue(s.add(new Fixed(2)));
		assertTrue(s.add(new Fixed(3)));
		assertTrue(s.remove(new Fixed(2)));

		assertTrue(s.contains(new Fixed(1)));
		assertTrue(s.contains(new Fixed(3)));
		assertFalse(s.contains(new Fixed(2)));
	}

	@Test
	void toArrayVariants() {
		var s = new SwissSet<String>();
		s.addAll(List.of("a", "b", "c"));
		Object[] arr = s.toArray();
		assertEquals(3, arr.length);
		assertTrue(Arrays.asList(arr).containsAll(Set.of("a", "b", "c")));

		String[] target = new String[0];
		String[] out = s.toArray(target);
		assertEquals(3, out.length);
		assertTrue(Set.of(out).containsAll(Set.of("a", "b", "c")));
	}

	@Test
	void iteratorCoversAll() {
		var s = new SwissSet<Integer>();
		int n = 1_000;
		for (int i = 0; i < n; i++) s.add(i);

		int count = 0;
		long sum = 0;
		for (int v : s) {
			count++;
			sum += v;
		}
		assertEquals(n, count);
		assertEquals((long) (n - 1) * n / 2, sum);
	}
}

