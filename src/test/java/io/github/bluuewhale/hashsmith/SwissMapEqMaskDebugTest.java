package io.github.bluuewhale.hashsmith;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * A "visually inspectable" debug test for eqMask.
 *
 * <p>This test calls the real {@link SwissMap#eqMask(long, byte)} implementation to verify the
 * packed 8-bit match mask.
 */
class SwissMapEqMaskDebugTest {
	private final SwissMap<Object, Object> map = new SwissMap<>();

	int eqMask(long word, byte b) {
		return map.eqMask(word, b);
	}

    long pack8(byte b0, byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7) {
        return (b0 & 0xFFL)
            | ((b1 & 0xFFL) << 8)
            | ((b2 & 0xFFL) << 16)
            | ((b3 & 0xFFL) << 24)
            | ((b4 & 0xFFL) << 32)
            | ((b5 & 0xFFL) << 40)
            | ((b6 & 0xFFL) << 48)
            | ((b7 & 0xFFL) << 56);
    }

    String bits8(int mask) {
        String s = Integer.toBinaryString(mask & 0xFF);
        if (s.length() < 8) s = "0".repeat(8 - s.length()) + s;
        return s;
    }

	@Test
	void testEqMask() {
		long word = pack8((byte) 0xBB, (byte) 0xAA, (byte) 0xBB, (byte) 0xAA, (byte) 0xBB, (byte) 0xBB, (byte) 0xBB, (byte) 0xBB);

		assertEquals("00001010", bits8(eqMask(word, (byte) 0xAA)));
		assertEquals("11110101", bits8(eqMask(word, (byte) 0xBB)));
	}

	@Test
	void testEqMaskAllZero() {
		long word = pack8((byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00);

		assertEquals("11111111", bits8(eqMask(word, (byte) 0x00)));
		assertEquals("00000000", bits8(eqMask(word, (byte) 0x01)));
	}

	@Test
	void testEqMaskEdgeCase_Qword0100() {
		long word = 0x0000_0000_0000_0100L;

		int mask = eqMask(word, (byte) 0x00);

		// bytes: [00, 01, 00, 00, 00, 00, 00, 00] -> mask bits [1,0,1,1,1,1,1,1] = 0xFD
		assertEquals(0xFD, mask & 0xFF);
		assertEquals("11111101", bits8(mask));
	}
}


