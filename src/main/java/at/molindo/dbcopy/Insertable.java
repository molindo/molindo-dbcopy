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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import at.molindo.dbcopy.util.NaturalRowComparator;

import com.mysql.jdbc.Statement;

public interface Insertable extends Selectable {

	/**
	 * @return a {@link NaturalRowComparator} for rows
	 */
	NaturalRowComparator getComparator();

	/**
	 * @return the number of rows for a bulk insert
	 * @see #getBulkInsertQuery()
	 */
	int getBulkSize();

	/**
	 * @return {@link PreparedStatement} query for inserts
	 */
	String getInsertQuery();

	/**
	 * @return {@link PreparedStatement} query for bulk inserts. This is
	 *         typically much faster than relying on
	 *         {@link Statement#addBatch(String)}.
	 * @see #getBulkSize()
	 */
	String getBulkInsertQuery();

	/**
	 * @return {@link PreparedStatement} query for updates. This query updates
	 *         the key itself too support cases where the value is equal but not
	 *         the same, e.g.
	 *         "UPDATE t1 SET id = 'foo', value = 'bar' where id = 'Foo';"
	 */
	String getUpdateQuery();

	/**
	 * @return {@link PreparedStatement} query for deletes
	 */
	String getDeleteQuery();

	/**
	 * populate an insert query with values
	 * 
	 * @see #getInsertQuery()
	 */
	void insert(PreparedStatement insert, Object[] values) throws SQLException;

	/**
	 * populate a bulk insert query with values
	 * 
	 * @see #getBulkInsertQuery()
	 */
	void insert(PreparedStatement bulkInsert, Object[] values, int bulkPosition) throws SQLException;

	/**
	 * populate an update query with values
	 * 
	 * @see #getUpdateQuery()
	 */
	void update(PreparedStatement update, Object[] values) throws SQLException;

	/**
	 * populate a delete query with values
	 * 
	 * @see #getDeleteQuery()
	 */
	void delete(PreparedStatement delete, Object[] values) throws SQLException;

}
