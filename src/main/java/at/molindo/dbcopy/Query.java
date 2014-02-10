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

import at.molindo.utils.data.StringUtils;

/**
 * A {@link Selectable} implementation based on a simple SQL Query (read-only).
 */
public class Query implements Selectable {

	private final String _name;
	private final String _query;

	public Query(String query) {
		this(null, query);
	}

	public Query(String name, String query) {
		if (StringUtils.empty(query)) {
			throw new IllegalArgumentException("query must not be empty");
		}
		_name = StringUtils.empty(name) ? query : name;
		_query = query;
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public String getOrderedSelect() {
		return _query;
	}

}
