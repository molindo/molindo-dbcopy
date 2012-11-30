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

import static at.molindo.utils.collections.IteratorUtils.transform;
import static at.molindo.utils.data.StringUtils.join;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import at.molindo.dbcopy.util.CollationRowComparator;
import at.molindo.dbcopy.util.NaturalRowComparator;
import at.molindo.utils.data.Function;

public class Table {
	private static final int DEFAULT_BULK_SIZE = 5000;

	private final String _name;
	private final String[] _columns;
	private final String[] _pkColumnNames;
	private final int[] _pkColumnIndexes;
	private NaturalRowComparator _comparator;
	private final int _bulkSize = DEFAULT_BULK_SIZE;

	private final String _select;
	private final String _insert;
	private final String _bulkInsert;
	private final String _update;
	private final String _delete;

	public Table(String name, List<String> columns, List<String> pkColumns, Map<String, String> pkColumnCollations) {
		if (columns.isEmpty()) {
			throw new IllegalArgumentException("columns must not be empty (" + name + ")");
		}
		if (pkColumns.isEmpty()) {
			throw new IllegalArgumentException("pkColumns must not be empty (" + name + ")");
		}
		_name = name;
		_columns = columns.toArray(new String[columns.size()]);
		_pkColumnNames = pkColumns.toArray(new String[pkColumns.size()]);

		// column indexes of primary key columns
		_pkColumnIndexes = new int[_pkColumnNames.length];
		for (int i = 0; i < _pkColumnNames.length; i++) {
			_pkColumnIndexes[i] = getColumnIndex(_pkColumnNames[i]);
		}

		// comparator
		if (pkColumnCollations.isEmpty()) {
			_comparator = new NaturalRowComparator(_pkColumnIndexes);
		} else {
			String[] collations = new String[_pkColumnNames.length];
			for (int i = 0; i < _pkColumnNames.length; i++) {
				collations[i] = pkColumnCollations.get(_pkColumnNames[i]);
			}
			_comparator = new CollationRowComparator(_pkColumnIndexes, collations);
		}

		// value list
		String columnList = join(",", transform(columns, new Function<String, String>() {

			@Override
			public String apply(String column) {
				return "`" + column + "`";
			}
		}));

		// placeholders
		String placeholders = join(",", transform(columns, new Function<String, String>() {

			@Override
			public String apply(String column) {
				return "?";
			}
		}));

		// update assignments
		String assignments = join(",", transform(columns, new Function<String, String>() {

			@Override
			public String apply(String column) {
				return "`" + column + "` = ?";
			}
		}));

		// where clause
		String where = join(" AND ", transform(pkColumns, new Function<String, String>() {

			@Override
			public String apply(String column) {
				return "`" + column + "` = ?";
			}
		}));

		// ordered select query
		_select = "SELECT " + columnList + "  FROM `" + _name + "` ORDER BY "
				+ join(",", transform(pkColumns, new Function<String, String>() {

					@Override
					public String apply(String column) {
						return "`" + column + "`";
					}
				}));

		// insert query
		_insert = "INSERT INTO `" + name + "` VALUES (" + placeholders + ")";

		// bulk insert query
		StringBuilder buf = new StringBuilder("INSERT INTO `" + name + "` VALUES ");
		for (int i = 0; i < _bulkSize; i++) {
			buf.append("(").append(placeholders).append("),\n");
		}
		buf.setLength(buf.length() - 2);
		_bulkInsert = buf.toString();

		// update query
		_update = "UPDATE `" + name + "` SET " + assignments + " WHERE " + where;

		// delete query
		_delete = "DELETE FROM `" + name + "` WHERE " + where;

	}

	public String getName() {
		return _name;
	}

	public String getOrderedSelect() {
		return _select;
	}

	public String getInsertQuery() {
		return _insert;
	}

	public String getBulkInsertQuery() {
		return _bulkInsert;
	}

	public void insert(PreparedStatement ps, Object[] row) throws SQLException {
		insert(ps, row, 0);
	}

	public void insert(PreparedStatement ps, Object[] row, int bulkPosition) throws SQLException {
		if (row.length != _columns.length) {
			throw new IllegalArgumentException("row size does not match number of columns");
		}

		int bulkOffset = bulkPosition * row.length;
		for (int i = 0; i < row.length; i++) {
			ps.setObject(bulkOffset + i + 1, row[i]);
		}
	}

	public String getUpdateQuery() {
		return _update;
	}

	public void update(PreparedStatement ps, Object[] row) throws SQLException {
		/*
		 * NOTE: some primary key values might change despite still being equal
		 * (collations!) so we update everything
		 */

		if (row.length != _columns.length) {
			throw new IllegalArgumentException("row size does not match number of columns");
		}

		// set
		for (int i = 0; i < _columns.length; i++) {
			ps.setObject(i + 1, row[i]);
		}

		// where
		for (int i = 0; i < _pkColumnIndexes.length; i++) {
			ps.setObject(i + 1, row[_pkColumnIndexes[i]]);
		}
	}

	public String getDeleteQuery() {
		return _delete;
	}

	public void delete(PreparedStatement ps, Object[] row) throws SQLException {
		if (row.length != _columns.length) {
			throw new IllegalArgumentException("row size does not match number of columns");
		}

		for (int i = 0; i < _pkColumnIndexes.length; i++) {
			ps.setObject(i + 1, row[_pkColumnIndexes[i]]);
		}
	}

	@SuppressWarnings("unused")
	private String getColumnName(int idx) {
		return _columns[idx];
	}

	private int getColumnIndex(String name) {
		for (int i = 0; i < _columns.length; i++) {
			if (_columns[i].equals(name)) {
				return i;
			}
		}
		throw new IllegalArgumentException("unknown column name " + name);
	}

	public NaturalRowComparator getComparator() {
		return _comparator;
	}

	public int getBulkSize() {
		return _bulkSize;
	}
}
