/**
 * Copyright 2010 Molindo GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.molindo.dbcopy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

public class CollationRowComparatorTest {

	private static Comparator<String> newComparator() {
		return CollationRowComparator.newComparator("utf8_unicode_ci");
	}

	@Test
	public void testUnicode() {
		Comparator<String> c = newComparator();
		assertEquals(0, c.compare("Foobar", "foobar"));
		assertEquals(0, c.compare("fußball", "Fussball"));
		assertEquals(0, c.compare("Café", "cafe"));

		assertEquals(-1, c.compare("´", "’")); // wrong order
		assertEquals(-1, c.compare("l", "ł")); // not equal in MySQL
	}

	@Test
	public void testPerformance() {
		int count = 10000;

		List<String> list = new ArrayList<String>(count);
		for (int i = 0; i < count; i++) {
			list.add(UUID.randomUUID().toString());
		}

		long start = System.currentTimeMillis();
		Collections.sort(list, newComparator());

		long time = System.currentTimeMillis() - start;

		assertTrue("took " + time + "ms", time < 1000);
	}
}
