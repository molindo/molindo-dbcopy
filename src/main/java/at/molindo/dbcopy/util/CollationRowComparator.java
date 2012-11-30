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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import at.molindo.mysqlcollations.lib.CollationComparator;
import at.molindo.utils.collections.CollectionUtils;

public class CollationRowComparator extends NaturalRowComparator {

	private static final long serialVersionUID = 1L;
	// private static final Set<String> UNICODE_CHARSETS =
	// CollectionUtils.unmodifiableSet("utf8", "utf8mb3", "utf8mb4",
	// "utf16", "utf32", "ucs2");

	private final List<Comparator<String>> _collators;

	static Comparator<String> newComparator(String collation) {
		return CollationComparator.comparator(collation);
	}

	public CollationRowComparator(int[] indexes, String[] collations) {
		super(indexes);

		if (indexes.length != collations.length) {
			throw new IllegalArgumentException("indexes and collations must be of same length");
		}

		_collators = new ArrayList<Comparator<String>>(collations.length);
		CollectionUtils.resize(_collators, collations.length);

		for (int i = 0; i < collations.length; i++) {
			String collation = collations[i];
			if (collation != null) {
				_collators.set(i, newComparator(collation));
			}
		}

	}

	@Override
	protected int compare(int i, Object c1, Object c2) {
		Comparator<String> collator = _collators.get(i);
		if (collator != null) {
			return collator.compare((String) c1, (String) c2);
		} else {
			return super.compare(i, c1, c2);
		}
	}

	// /**
	// * "All MySQL collations are of type PADSPACE. This means that all CHAR
	// and
	// * VARCHAR values in MySQL are compared without regard to any trailing
	// * spaces."
	// */
	// private static final class PadSpaceComparator implements
	// Comparator<String>, Serializable {
	//
	// private static final long serialVersionUID = 1L;
	// private final Comparator<? super String> _wrapped;
	//
	// private PadSpaceComparator(Comparator<? super String> wrapped) {
	// if (wrapped == null) {
	// throw new NullPointerException("wrapped");
	// }
	// _wrapped = wrapped;
	// }
	//
	// @Override
	// public int compare(String o1, String o2) {
	// return _wrapped.compare(StringUtils.trimTrailing(o1),
	// StringUtils.trimTrailing(o2));
	// }
	//
	// }
}
