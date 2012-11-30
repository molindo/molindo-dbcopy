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

import java.io.Serializable;
import java.util.Comparator;

/**
 * compare rows by their public keys. must be identical to sorting by DB itself
 */
public class NaturalRowComparator implements Comparator<Object[]>, Serializable {

	private static final long serialVersionUID = 1L;

	private final int[] _indexes;

	public NaturalRowComparator(int[] indexes) {
		if (indexes == null || indexes.length == 0) {
			throw new IllegalArgumentException("no comparable columns");
		}
		_indexes = indexes;
	}

	@Override
	public final int compare(Object[] o1, Object[] o2) {
		if (o1.length == 0) {
			return o2.length == 0 ? 0 : 1;
		} else if (o2.length == 0) {
			return -1;
		}

		for (int i = 0; i < _indexes.length; i++) {
			int idx = _indexes[i];
			int cmp = compare(i, o1[idx], o2[idx]);
			if (cmp != 0) {
				return cmp;
			}
		}

		return 0;
	}

	/**
	 * 
	 * @param i
	 *            the primary key index, i.e. not the column index
	 * @param c1
	 *            the first object to compare
	 * @param c2
	 *            the second object to compare
	 * @see Comparator#compare(Object, Object)
	 */
	@SuppressWarnings("unchecked")
	protected int compare(int i, Object c1, Object c2) {
		if (c1 == null || c2 == null) {
			// TODO in SQL NULL is not equal to NULL which makes order
			// unpredictable
			throw new RuntimeException("order by null not implemented");
		}
		// FIXME not sufficient for strings (collations!)
		int cmp = ((Comparable<Object>) c1).compareTo(c2);
		return cmp;
	}

}
