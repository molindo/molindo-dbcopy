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
package at.molindo.dbcopy;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import at.molindo.utils.data.StringUtils;

/**
 * A Column is a collection of metadata for a column in a {@link Table}
 * (read-only)
 */
public class Column {

	private final String _name;
	private final String _collation;
	private final Class<?> _javaType;

	public Column(String name) {
		this(name, null, null);
	}

	public Column(String name, String collation) {
		this(name, collation, null);
	}

	public Column(String name, Class<?> javaType) {
		this(name, null, javaType);
	}

	public Column(String name, String collation, Class<?> javaType) {
		if (StringUtils.empty(name)) {
			throw new IllegalArgumentException("column name must not be empty");
		}
		_name = name;
		_collation = collation;
		_javaType = javaType;
	}

	@Nonnull
	public String getName() {
		return _name;
	}

	@CheckForNull
	public String getCollation() {
		return _collation;
	}

	@CheckForNull
	public Class<?> getJavaType() {
		return _javaType;
	}

	@Override
	public String toString() {
		return "Column [" + _name + (_collation != null ? ", collation=" + _collation : "")
				+ (_javaType != null ? ", javaType=" + _javaType : "") + "]";
	}
}
