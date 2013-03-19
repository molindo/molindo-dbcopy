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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import at.molindo.dbcopy.util.Equals.ValueEquals;

public class EqualsTest {

	private static final Short S42 = 42;
	private static final Integer I42 = 42;
	private static final Long L42 = 42L;
	private static final Float F42_5 = 42.5F;
	private static final Double D42_5 = 42.5;

	@Test
	public void testLong() {
		ValueEquals e = Equals.findEquals(Short.class, Long.class);
		assertSame(ValueEquals.LONG, e);
		equals(e, S42, L42);
		equals(e, D42_5, L42); // compared as Long
		notEquals(e, I42, 41.8);
	}

	@Test
	public void testDouble() {
		ValueEquals e = Equals.findEquals(Short.class, Double.class);
		assertSame(ValueEquals.DOUBLE, e);
		equals(e, S42, L42);
		equals(e, F42_5, D42_5);
		notEquals(e, F42_5, L42);
	}

	@Test
	public void testDefault() {
		ValueEquals e = Equals.findEquals(Short.class, String.class);
		assertSame(ValueEquals.DEFAULT, e);
		notEquals(e, S42, L42);
		equals(e, F42_5, F42_5);
	}

	private static void equals(ValueEquals e, Object o1, Object o2) {
		assertTrue(e.equals(o1, o2));
	}

	private static void notEquals(ValueEquals e, Object o1, Object o2) {
		assertFalse(e.equals(o1, o2));
	}
}
