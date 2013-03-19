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
package at.molindo.dbcopy.handler;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SimpleKeyedListHandler<K> extends AbstractLinkedKeyedHandler<K, Object[]> {

	private final String _name;
	private final int _index;

	public SimpleKeyedListHandler(String column) {
		_name = column;
		_index = -1;
	}

	public SimpleKeyedListHandler(int column) {
		_name = null;
		_index = column;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected K createKey(ResultSet rs) throws SQLException {
		return (K) (_name != null ? rs.getObject(_name) : rs.getObject(_index));
	}

	@Override
	protected Object[] createRow(ResultSet rs) throws SQLException {
		Object[] row = new Object[rs.getMetaData().getColumnCount()];

		for (int i = 0; i < row.length; i++) {
			row[i] = rs.getObject(i + 1);
		}

		return row;
	}
}
