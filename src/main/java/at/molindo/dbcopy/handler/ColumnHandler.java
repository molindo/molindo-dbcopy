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

import at.molindo.dbcopy.Column;

/**
 * a list of columns, expecting rows:
 * 
 * <ol> <li>name</li> <li>collation</li> </ol>
 */
public class ColumnHandler extends AbstractLinkedKeyedHandler<String, Column> {

	@Override
	protected String createKey(ResultSet rs) throws SQLException {
		return rs.getString(1);
	}

	@Override
	protected Column createRow(ResultSet rs) throws SQLException {
		return new Column(rs.getString(1), rs.getString(2));
	}
}
