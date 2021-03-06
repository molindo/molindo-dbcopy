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
package at.molindo.dbcopy.operation;

import java.util.Queue;

import at.molindo.dbcopy.util.Utils;
import at.molindo.utils.collections.ArrayUtils;

public abstract class Operation {

	/**
	 * indicates the end of operations in a {@link Queue}
	 */
	public static final Operation END = new Operation() {
	};

	private final Object[] _values;

	/**
	 * {@link #END} only
	 */
	private Operation() {
		_values = Utils.END;
	}

	public Operation(Object[] values) {
		if (ArrayUtils.empty(values)) {
			throw new IllegalArgumentException("values must not be empty");
		}
		_values = values;
	}

	public final Object[] getValues() {
		return _values;
	}

}
