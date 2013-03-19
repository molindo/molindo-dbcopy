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

public class SimpleKeyedHandler<K, V> extends AbstractLinkedKeyedHandler<K, V> {

	public static final SimpleKeyedHandler<String, Object> STRING_OBJECT = new SimpleKeyedHandler<String, Object>();
	public static final SimpleKeyedHandler<String, String> STRING_STRING = new SimpleKeyedHandler<String, String>();

	/**
	 * specialized handler for system variables, treating "" as null
	 */
	public static final SimpleKeyedHandler<String, Object> VARIABLES = new SimpleKeyedHandler<String, Object>() {
		@Override
		protected String createRow(ResultSet rs) throws SQLException {
			String value = (String) super.createRow(rs);
			return "".equals(value) ? null : value;
		}
	};

	@Override
	@SuppressWarnings("unchecked")
	protected K createKey(ResultSet rs) throws SQLException {
		return (K) rs.getObject(1);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected V createRow(ResultSet rs) throws SQLException {
		return (V) rs.getObject(2);
	}

}
