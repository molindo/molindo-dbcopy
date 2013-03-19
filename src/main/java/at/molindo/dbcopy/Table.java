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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import at.molindo.dbcopy.util.CollationRowComparator;
import at.molindo.dbcopy.util.NaturalRowComparator;
import at.molindo.utils.collections.ArrayUtils;
import at.molindo.utils.collections.CollectionUtils;
import at.molindo.utils.collections.IteratorUtils;
import at.molindo.utils.data.Function;
import at.molindo.utils.data.StringUtils;

public class Table implements Insertable {
	private static final int DEFAULT_BULK_SIZE = 5000;

	private static final String PRIMARY_KEY_NAME = "PRIMARY";

	private final String _name;
	private final String[] _columns;
	private final Map<String, UniqueKey> _uniquKeys;
	private UniqueKey _primaryKey;

	private final int _bulkSize = DEFAULT_BULK_SIZE;

	private final String _insert;
	private final String _bulkInsert;

	public static Builder builder(String tableName) {
		return new Builder(tableName);
	}

	private static <T> String string(String separator, T[] array, Function<T, String> f) {
		return string(separator, ArrayUtils.iterable(array), f);
	}

	private static <T> String string(String separator, Iterable<T> iter, Function<T, String> f) {
		return StringUtils.join(separator, IteratorUtils.transform(iter, f));
	}

	private Table(String name, List<Column> columns, Map<String, List<Column>> uniqueKeys) {
		if (columns.isEmpty()) {
			throw new IllegalArgumentException("columns must not be empty (" + name + ")");
		}
		if (uniqueKeys.isEmpty()) {
			throw new IllegalArgumentException("unique keys must not be empty (" + name + ")");
		}
		_name = name;
		_columns = new String[columns.size()];
		for (int i = 0; i < _columns.length; i++) {
			_columns[i] = columns.get(i).getName();
		}

		// placeholders
		String placeholders = string(",", columns, new Function<Column, String>() {

			@Override
			public String apply(Column column) {
				return "?";
			}
		});

		// insert query
		_insert = "INSERT INTO `" + name + "` VALUES (" + placeholders + ")";

		// bulk insert query
		StringBuilder buf = new StringBuilder("INSERT INTO `" + name + "` VALUES ");
		for (int i = 0; i < _bulkSize; i++) {
			buf.append("(").append(placeholders).append("),\n");
		}
		buf.setLength(buf.length() - 2);
		_bulkInsert = buf.toString();

		_uniquKeys = new HashMap<String, Table.UniqueKey>();
		for (Map.Entry<String, List<Column>> e : uniqueKeys.entrySet()) {
			_uniquKeys.put(e.getKey(), new UniqueKey(e.getKey(), e.getValue()));
		}

		_primaryKey = _uniquKeys.size() == 1 ? CollectionUtils.firstValue(_uniquKeys) : _uniquKeys
				.get(PRIMARY_KEY_NAME);

	}

	public UniqueKey getIndex(@Nullable String index) {
		if (StringUtils.empty(index)) {
			if (_primaryKey == null) {
				throw new IllegalArgumentException("no default index available for " + _name);
			} else {
				return _primaryKey;
			}
		} else {
			UniqueKey idx = _uniquKeys.get(index);
			if (idx == null) {
				throw new IllegalArgumentException("index unknown for " + _name + ": " + index);
			}
			return idx;
		}
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public String getOrderedSelect() {
		return getPrimaryKey().getOrderedSelect();
	}

	@Override
	public String getInsertQuery() {
		return _insert;
	}

	@Override
	public String getBulkInsertQuery() {
		return _bulkInsert;
	}

	@Override
	public void insert(PreparedStatement ps, Object[] row) throws SQLException {
		insert(ps, row, 0);
	}

	@Override
	public void insert(PreparedStatement ps, Object[] row, int bulkPosition) throws SQLException {
		if (row.length != _columns.length) {
			throw new IllegalArgumentException("row size does not match number of columns");
		}

		int bulkOffset = bulkPosition * row.length;
		for (int i = 0; i < row.length; i++) {
			ps.setObject(bulkOffset + i + 1, row[i]);
		}
	}

	@Override
	public String getUpdateQuery() {
		return getPrimaryKey().getUpdateQuery();
	}

	@Override
	public void update(PreparedStatement ps, Object[] row) throws SQLException {
		getPrimaryKey().update(ps, row);
	}

	@Override
	public String getDeleteQuery() {
		return getPrimaryKey().getDeleteQuery();
	}

	@Override
	public void delete(PreparedStatement ps, Object[] row) throws SQLException {
		getPrimaryKey().delete(ps, row);
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

	@Override
	public NaturalRowComparator getComparator() {
		return getPrimaryKey().getComparator();
	}

	@Override
	public int getBulkSize() {
		return _bulkSize;
	}

	public UniqueKey getPrimaryKey() {
		if (_primaryKey == null) {
			throw new IllegalStateException("table " + _name + " does not have a primary key");
		}
		return _primaryKey;
	}

	private class UniqueKey implements Insertable {
		private final String _indexName;
		private final String[] _pkColumnNames;
		private final int[] _pkColumnIndexes;
		private final NaturalRowComparator _comparator;

		private final String _select;
		private final String _update;
		private final String _delete;

		private UniqueKey(String indexName, List<Column> pkColumns) {
			if (StringUtils.empty(indexName)) {
				throw new IllegalArgumentException("indexName must not be empty");
			}
			if (pkColumns.isEmpty()) {
				throw new IllegalArgumentException("pkColumns must not be empty (" + _name + ")");
			}

			_indexName = indexName;

			_pkColumnNames = new String[pkColumns.size()];
			String[] pkColumnCollations = null;

			for (int i = 0; i < _pkColumnNames.length; i++) {
				Column col = pkColumns.get(i);
				_pkColumnNames[i] = col.getName();
				if (col.getCollation() != null) {
					if (pkColumnCollations == null) {
						pkColumnCollations = new String[_pkColumnNames.length];
					}
					pkColumnCollations[i] = col.getCollation();
				}
			}

			// column indexes of primary key columns
			_pkColumnIndexes = new int[_pkColumnNames.length];
			for (int i = 0; i < _pkColumnNames.length; i++) {
				_pkColumnIndexes[i] = getColumnIndex(_pkColumnNames[i]);
			}

			// comparator
			if (pkColumnCollations != null) {
				_comparator = new CollationRowComparator(_pkColumnIndexes, pkColumnCollations);
			} else {
				_comparator = new NaturalRowComparator(_pkColumnIndexes);
			}

			// value list
			String columnList = string(",", _columns, new Function<String, String>() {

				@Override
				public String apply(String column) {
					return "`" + column + "`";
				}
			});

			// update assignments
			String assignments = string(",", _columns, new Function<String, String>() {

				@Override
				public String apply(String column) {
					return "`" + column + "` = ?";
				}
			});

			// where clause
			String where = string(" AND ", pkColumns, new Function<Column, String>() {

				@Override
				public String apply(Column column) {
					return "`" + column.getName() + "` = ?";
				}
			});

			// ordered select query
			_select = "SELECT " + columnList + "  FROM `" + _name + "` ORDER BY "
					+ string(",", pkColumns, new Function<Column, String>() {

						@Override
						public String apply(Column column) {
							return "`" + column.getName() + "`";
						}
					});

			// update query
			_update = "UPDATE `" + _name + "` SET " + assignments + " WHERE " + where;

			// delete query
			_delete = "DELETE FROM `" + _name + "` WHERE " + where;
		}

		@Override
		public String getOrderedSelect() {
			return _select;
		}

		@Override
		public String getUpdateQuery() {
			return _update;
		}

		@Override
		public void update(PreparedStatement ps, Object[] row) throws SQLException {
			/*
			 * NOTE: some primary key values might change despite still being
			 * equal (collations!) so we update everything
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
				ps.setObject(_columns.length + i + 1, row[_pkColumnIndexes[i]]);
			}
		}

		@Override
		public String getDeleteQuery() {
			return _delete;
		}

		@Override
		public void delete(PreparedStatement ps, Object[] row) throws SQLException {
			if (row.length != _columns.length) {
				throw new IllegalArgumentException("row size does not match number of columns");
			}

			for (int i = 0; i < _pkColumnIndexes.length; i++) {
				ps.setObject(i + 1, row[_pkColumnIndexes[i]]);
			}
		}

		@Override
		public NaturalRowComparator getComparator() {
			return _comparator;
		}

		@Override
		public String getName() {
			return _name + "." + _indexName;
		}

		@Override
		public int getBulkSize() {
			return Table.this.getBulkSize();
		}

		@Override
		public String getInsertQuery() {
			return Table.this.getInsertQuery();
		}

		@Override
		public String getBulkInsertQuery() {
			return Table.this.getBulkInsertQuery();
		}

		@Override
		public void insert(PreparedStatement insert, Object[] values) throws SQLException {
			Table.this.insert(insert, values);
		}

		@Override
		public void insert(PreparedStatement bulkInsert, Object[] values, int bulkPosition) throws SQLException {
			Table.this.insert(bulkInsert, values, bulkPosition);

		}
	}

	public static final class Builder {

		private final String _name;
		private final List<Column> _columns = new ArrayList<Column>(20);
		private final Map<String, List<Column>> _uniqueKeys = new HashMap<String, List<Column>>();

		private Builder(String tableName) {
			if (StringUtils.empty(tableName)) {
				throw new IllegalArgumentException("table name must not be empty");
			}
			_name = tableName;
		}

		public Builder addColumns(Collection<Column> columns) {
			_columns.addAll(columns);
			return this;
		}

		public Builder addUniqueKey(String name, List<Column> columns) {
			if (_uniqueKeys.put(name, columns) != null) {
				throw new IllegalArgumentException("duplicate key name: " + name);
			}
			return this;
		}

		public Table build() {
			return new Table(_name, _columns, _uniqueKeys);
		}

	}
}
