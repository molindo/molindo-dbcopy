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

import org.junit.Test;

public class RowComparatorTest {

	@Test
	public void test() {
		NaturalRowComparator c = new NaturalRowComparator(new int[] { 0 });

		assertEquals(0, c.compare(row(1), row(1)));

		// a negative integer as the first argument is less than the second
		assertTrue(c.compare(row(1), row(2)) < 0);
		// a positive integer as the first argument is greater than the second
		assertTrue(c.compare(row(2), row(1)) > 0);

		// test empty row as end of table replacement
		assertTrue(c.compare(row(1), row()) < 0);
		assertTrue(c.compare(row(), row(1)) > 0);
		assertEquals(0, c.compare(row(), row()));

	}

	private static Object[] row(Object... row) {
		return row;
	}
}
