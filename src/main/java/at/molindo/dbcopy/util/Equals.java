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

import java.sql.ResultSet;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import at.molindo.dbcopy.Column;
import at.molindo.dbcopy.task.SelectReader;
import at.molindo.utils.collections.ClassMap;

/**
 * check equality of rows using {@link ResultSet} header information. Also
 * supports equality for some compatible types (e.g. Integer and Long)
 */
public class Equals {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Equals.class);

	/**
	 * {@link ValueEquals} instance for subtypes of {@link Number}
	 */
	private static final ClassMap<ValueEquals> NUMERIC;

	static {
		NUMERIC = new ClassMap<Equals.ValueEquals>();
		NUMERIC.put(Short.class, ValueEquals.SHORT);
		NUMERIC.put(Integer.class, ValueEquals.INTEGER);
		NUMERIC.put(Long.class, ValueEquals.LONG);
		NUMERIC.put(Float.class, ValueEquals.FLOAT);
		NUMERIC.put(Double.class, ValueEquals.DOUBLE);
	}

	private final ValueEquals[] _equals;

	@Nonnull
	public static ValueEquals findEquals(@Nonnull Class<?> c1, @Nonnull Class<?> c2) {
		if (c1 == null) {
			throw new NullPointerException("c1");
		}
		if (c2 == null) {
			throw new NullPointerException("c2");
		}

		// TODO support for arrays necessary (e.g. byte[])?
		if (c1.equals(c2)) {
			return ValueEquals.DEFAULT;
		} else {
			ValueEquals e;

			// TODO add more compatible types
			if (Number.class.isAssignableFrom(c1) && Number.class.isAssignableFrom(c2)) {
				e = findNumberEquals(c1, c2);
			} else {
				e = ValueEquals.DEFAULT;
			}

			if (e == ValueEquals.DEFAULT) {
				log.warn("comparing incompatible types " + c1.getName() + " and " + c2.getName());
			} else if (log.isTraceEnabled()) {
				log.trace("using " + e + " to check equality for " + c1.getName() + " and " + c2.getName());
			}
			return e;
		}
	}

	/**
	 * returns bet fitting of two numeric equals, e.g.:
	 * 
	 * <ul> <li>INTEGER for Short and Integer</li> <li>FLOAT for Integer and
	 * Float</li> <li>DOUBLE for Short and Double</li> <li>...</li> </ul>
	 * 
	 * @param c1
	 * @param c2
	 * @return
	 */
	@Nonnull
	private static ValueEquals findNumberEquals(Class<?> c1, Class<?> c2) {
		ValueEquals e1 = NUMERIC.find(c1);
		ValueEquals e2 = NUMERIC.find(c2);

		ValueEquals e;
		if (e1 == null || e2 == null) {
			// TODO implement better fallback for unknown Number types
			e = ValueEquals.DEFAULT;
		} else {
			e = e1.compareTo(e2) > 0 ? e1 : e2;
		}

		return e;
	}

	/**
	 * 
	 * @param h1
	 *            a result header as created by {@link SelectReader}
	 * @param h2
	 *            a result header as created by {@link SelectReader} of same
	 *            length as h1
	 */
	public Equals(Object[] h1, Object[] h2) {
		if (h1.length != h2.length) {
			throw new IllegalArgumentException("header lengths must be equal");
		}

		_equals = new ValueEquals[h1.length];
		for (int i = 0; i < _equals.length; i++) {
			_equals[i] = findEquals(((Column) h1[i]).getJavaType(), ((Column) h2[i]).getJavaType());
		}

	}

	public boolean equals(Object[] r1, Object[] r2) {
		if (r1 == r2) {
			return true;
		}
		if (r1 == null || r2 == null) {
			return false;
		}

		int length = r1.length;
		if (r2.length != length) {
			return false;
		}

		for (int i = 0; i < length; i++) {
			if (!_equals[i].equals(r1[i], r2[i])) {
				return false;
			}
		}

		return true;
	}

	public enum ValueEquals {

		DEFAULT {
			@Override
			public boolean equals(Object o1, Object o2) {
				return o1 == null ? o2 == null : o1.equals(o2);
			}
		},

		SHORT {
			@Override
			public boolean equals(Object o1, Object o2) {
				return o1 == null ? o2 == null : ((Number) o1).shortValue() == ((Number) o2).shortValue();
			}
		},

		INTEGER {
			@Override
			public boolean equals(Object o1, Object o2) {
				return o1 == null ? o2 == null : ((Number) o1).intValue() == ((Number) o2).intValue();
			}
		},

		LONG {
			@Override
			public boolean equals(Object o1, Object o2) {
				return o1 == null ? o2 == null : ((Number) o1).longValue() == ((Number) o2).longValue();
			}
		},

		FLOAT {
			@Override
			public boolean equals(Object o1, Object o2) {
				return o1 == null ? o2 == null
						: Float.compare(((Number) o1).floatValue(), ((Number) o2).floatValue()) == 0;
			}
		},

		DOUBLE {
			@Override
			public boolean equals(Object o1, Object o2) {
				return o1 == null ? o2 == null : Double.compare(((Number) o1).doubleValue(),
						((Number) o2).doubleValue()) == 0;
			}
		};

		protected abstract boolean equals(@CheckForNull Object o1, @CheckForNull Object o2);
	}

}
