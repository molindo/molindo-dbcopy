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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbutils.handlers.MapListHandler;

import at.molindo.dbcopy.handler.ColumnHandler;
import at.molindo.dbcopy.handler.SimpleKeyedHandler;
import at.molindo.dbcopy.source.DataSource;
import at.molindo.dbcopy.source.DataSourceRole;
import at.molindo.dbcopy.source.DefaultConnectionPool;
import at.molindo.dbcopy.source.SynchronizedReadOnlyConnectionPool;
import at.molindo.dbcopy.task.AbstractConnectionRunnable;
import at.molindo.dbcopy.task.ConnectionExecutorService;
import at.molindo.dbcopy.util.SqlFunction;
import at.molindo.dbcopy.util.Utils;
import at.molindo.utils.collections.CollectionUtils;
import at.molindo.utils.collections.ListMap;
import at.molindo.utils.data.Function2;
import at.molindo.utils.data.ObjectUtils;

public class Database {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Database.class);

	private final int _poolSize;

	private final DefaultConnectionPool _dataSource;
	private final Map<String, Table> _tables;

	private DataSourceState _state;

	public Database(DataSourceRole mode, String jdbcUrl, String user, String password, int poolSize) {
		if (poolSize < 1) {
			throw new IllegalArgumentException("poolSize must be >= 1, was " + poolSize);
		}
		_poolSize = poolSize;

		_dataSource = mode.newConnectionPool(jdbcUrl, user, password, poolSize);

		// read table metadata
		_tables = readTables(_dataSource);
		if (log.isDebugEnabled()) {
			log.debug("finished reading table metadata for tables: " + getTableNames());
		}

		_state = new Initializing();
	}

	public void executeAll(String query) {
		_state.executeAll(query);
	}

	public <T> List<T> each(Function2<Connection, T, SQLException> f) {
		return _state.each(f);
	}

	public void start() {
		if (_state instanceof Initializing) {
			if (_dataSource instanceof SynchronizedReadOnlyConnectionPool) {
				// sync on all tables
				((SynchronizedReadOnlyConnectionPool) _dataSource).syncRead(_tables.keySet().toArray(
						new String[_tables.size()]));
			}
			_state = new Executing();
		} else {
			throw new IllegalStateException("not in initializing state");
		}
	}

	private static Map<String, Table> readTables(DataSource dataSource) {
		Connection c = dataSource.getConnection();
		try {
			return readTables(c);
		} catch (SQLException e) {
			throw new RuntimeException("reading table metadata failed", e);
		} finally {
			Utils.close(c);
		}
	}

	private static Map<String, Table> readTables(Connection connection) throws SQLException {
		Map<String, Table> tables = new HashMap<String, Table>();

		DatabaseMetaData meta = connection.getMetaData();
		String catalog = connection.getCatalog();

		// for each table in current catalog
		ResultSet rs = meta.getTables(catalog, null, null, null);
		try {
			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");

				Table.Builder table = Table.builder(tableName);

				// columns
				String columnsQuery = "select COLUMN_NAME,COLLATION_NAME from information_schema.COLUMNS where TABLE_SCHEMA=? and TABLE_NAME=? order by ORDINAL_POSITION";
				Map<String, Column> columns = Utils.executePrepared(connection, columnsQuery, new ColumnHandler(),
						catalog, tableName);
				if (columns.isEmpty()) {
					throw new IllegalStateException("table (" + tableName + ") without columns?");
				}
				table.addColumns(columns.values());

				// unique keys
				String uniqueKeysQuery = "show keys from `" + tableName + "` in `" + catalog
						+ "` where `Non_unique` = 0 and `Null` = ''";
				List<Map<String, Object>> uniqueKeyColumns = Utils.executePrepared(connection, uniqueKeysQuery,
						new MapListHandler());
				ListMap<String, Column> uniqeKeys = new ListMap<String, Column>();
				for (Map<String, Object> keyColumn : uniqueKeyColumns) {
					String name = (String) keyColumn.get("INDEX_NAME");
					String columnName = (String) keyColumn.get("COLUMN_NAME");

					if (name == null) {
						throw new IllegalStateException("KEY_NAME must not be null");
					}
					if (columnName == null) {
						throw new IllegalStateException("COLUMN_NAME must not be null");
					}

					Column column = columns.get(columnName);
					if (column == null) {
						throw new IllegalStateException("COLUMN_NAME unknown: " + columnName);
					}

					uniqeKeys.put(name, column);
				}
				for (Map.Entry<String, List<Column>> e : uniqeKeys.entrySet()) {
					table.addUniqueKey(e.getKey(), e.getValue());
				}
				if (uniqeKeys.isEmpty()) {
					log.warn("table without primary key not supported: " + tableName);
				} else {
					tables.put(tableName, table.build());
				}
			}
		} finally {
			Utils.close(rs);
		}

		return tables;
	}

	public Set<String> getTableNames() {
		return new HashSet<String>(_tables.keySet());
	}

	public void execute(AbstractConnectionRunnable runnable) {
		_state.execute(runnable);
	}

	public Future<?> submit(AbstractConnectionRunnable runnable) {
		return _state.submit(runnable);
	}

	public void close() {
		if (_state instanceof Executing) {
			try {
				((Executing) _state).close();
				_dataSource.close();
			} finally {
				_state = new Closed();
			}
		} else {
			throw new IllegalStateException("not in executing state");
		}
	}

	public Table getTable(String name) {
		Table t = _tables.get(name);
		if (t == null) {
			throw new IllegalArgumentException("unknown table " + name);
		}
		return t;
	}

	public Map<String, Object> getVariables(String... names) {
		return getVariables(CollectionUtils.set(names));
	}

	public Map<String, Object> getVariables(final Set<String> names) {
		Map<String, Object> vars = new HashMap<String, Object>();

		if (!CollectionUtils.empty(names)) {

			// sufficiently efficient implementation

			List<Map<String, Object>> allVars = each(new SqlFunction<Map<String, Object>>() {
				@Override
				public Map<String, Object> apply(Connection c) throws SQLException {
					return Utils.execute(c, "SHOW VARIABLES", SimpleKeyedHandler.VARIABLES);
				}
			});

			// init
			for (String name : names) {
				vars.put(name, allVars.get(0).get(name));
			}

			// check all for equality
			for (Map<String, Object> connectionVars : allVars.subList(1, allVars.size())) {
				for (String name : names) {
					Object connectionVal = connectionVars.get(name);
					if (!ObjectUtils.equals(connectionVal, vars.get(name))) {
						throw new IllegalStateException("variable " + name + " not equal for all connections");
					}
				}
			}
		}

		return vars;
	}

	public Map<String, Object> setVariables(Map<String, Object> variables) {
		if (!CollectionUtils.empty(variables)) {

			// sufficiently efficient implementation

			for (final Map.Entry<String, Object> e : variables.entrySet()) {
				each(new SqlFunction<Void>() {
					@Override
					public Void apply(Connection c) throws SQLException {
						Utils.executePrepared(c, "set `" + e.getKey() + "` = ?", null, e.getValue());
						return null;
					}
				});
			}

			// verify all for equality
			return getVariables(variables.keySet());
		} else {
			return new HashMap<String, Object>();
		}
	}

	private interface DataSourceState {

		void executeAll(String query);

		<T> List<T> each(Function2<Connection, T, SQLException> f);

		Future<?> submit(AbstractConnectionRunnable runnable);

		void execute(AbstractConnectionRunnable runnable);

	}

	private class Initializing implements DataSourceState {

		@Override
		public void executeAll(String query) {
			_dataSource.executeAll(query);
		}

		@Override
		public <T> List<T> each(Function2<Connection, T, SQLException> f) {
			return _dataSource.each(f);
		}

		@Override
		public void execute(AbstractConnectionRunnable runnable) {
			throw new IllegalStateException("not started");
		}

		@Override
		public Future<?> submit(AbstractConnectionRunnable runnable) {
			throw new IllegalStateException("not started");
		}

	}

	private class Executing implements DataSourceState {

		private final ConnectionExecutorService _exec;

		private Executing() {
			// create a thread pool, drains all connections from dataSource
			_exec = new ConnectionExecutorService(_poolSize, _dataSource);
		}

		@Override
		public void executeAll(String query) {
			throw new IllegalStateException("can't execute query after starting");
		}

		@Override
		public <T> List<T> each(Function2<Connection, T, SQLException> f) {
			throw new IllegalStateException("can't execute function after starting");
		}

		@Override
		public void execute(AbstractConnectionRunnable runnable) {
			_exec.execute(runnable);
		}

		@Override
		public Future<?> submit(AbstractConnectionRunnable runnable) {
			return _exec.submit(runnable);
		}

		public void close() {
			try {
				_exec.shutdown();
				_exec.awaitTermination(10, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				log.info("waiting for terminatinon of executor interrupted");
			}
		}
	}

	private static class Closed implements DataSourceState {
		@Override
		public void executeAll(String query) {
			throw new IllegalStateException("already closed");
		}

		@Override
		public <T> List<T> each(Function2<Connection, T, SQLException> f) {
			throw new IllegalStateException("already closed");
		}

		@Override
		public Future<?> submit(AbstractConnectionRunnable runnable) {
			throw new IllegalStateException("already closed");
		}

		@Override
		public void execute(AbstractConnectionRunnable runnable) {
			throw new IllegalStateException("already closed");
		}
	}

}
