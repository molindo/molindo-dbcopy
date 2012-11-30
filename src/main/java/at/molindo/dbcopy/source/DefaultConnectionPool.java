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
package at.molindo.dbcopy.source;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;

import at.molindo.dbcopy.util.ConnectionWrapper;
import at.molindo.dbcopy.util.SqlFunction;
import at.molindo.dbcopy.util.Utils;
import at.molindo.utils.data.Function2;

import com.mysql.jdbc.Driver;

public class DefaultConnectionPool implements DataSource {

	private final String _name;
	private final Queue<Connection> _connections;
	private final int _poolSize;

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DefaultConnectionPool.class);

	private static Queue<Connection> openConnections(String jdbcUrl, String user, String password, int poolSize) {
		try {
			// load driver
			Driver.class.getName();

			Properties props = new Properties();
			props.setProperty("user", user);
			props.setProperty("password", password);
			// MySQL properties
			props.setProperty("yearIsDateType", "false");

			LinkedList<Connection> connections = new LinkedList<Connection>();
			for (int i = 0; i < poolSize; i++) {
				Connection c = DriverManager.getConnection(jdbcUrl, (Properties) props.clone());
				connections.add(c);
			}
			return connections;

		} catch (SQLException e) {
			throw new RuntimeException("failed to open connections", e);
		}
	}

	public DefaultConnectionPool(String name, String jdbcUrl, String user, String password, int poolSize) {
		if (name == null) {
			throw new NullPointerException("name");
		}
		_name = name;
		_connections = openConnections(jdbcUrl, user, password, poolSize);
		_poolSize = poolSize;
	}

	@Override
	public String getName() {
		return _name;
	}

	public final synchronized <T> List<T> each(Function2<Connection, T, SQLException> f) {
		List<T> results = new ArrayList<T>(_connections.size());
		List<SQLException> exceptions = new ArrayList<SQLException>(_connections.size());
		for (Connection c : _connections) {
			try {
				results.add(f.apply(c));
			} catch (SQLException e) {
				exceptions.add(e);
			}
		}
		if (exceptions.size() > 0) {
			throw new RuntimeException("couldn't execute function on some or all connections", exceptions.get(0));
		} else {
			return results;
		}
	}

	public final void executeAll(final String query) {
		each(new SqlFunction<Void>() {

			@Override
			public Void apply(Connection c) throws SQLException {
				Utils.execute(c, query);
				return null;
			}
		});
	}

	@Override
	public final synchronized void close() {
		assertIdle();

		onBeforeClose();

		for (Connection c : _connections) {
			try {
				c.close();
			} catch (SQLException e) {
				log.warn("failed to close connection", e);
			}
		}
	}

	protected final void assertIdle() {
		if (_connections.size() != _poolSize) {
			throw new IllegalStateException("pool not idle");
		}
	}

	protected void onBeforeClose() {
	}

	@Override
	public synchronized Connection getConnection() {
		Connection c = _connections.poll();
		if (c == null) {
			throw new IllegalStateException("no connection available");
		}
		return new PooledConnection(c);
	}

	private synchronized void release(Connection c) {
		if (c == null) {
			throw new NullPointerException("c");
		}
		if (!_connections.offer(c)) {
			throw new RuntimeException("can't add connection to unbounded queue!?");
		}
	}

	private class PooledConnection extends ConnectionWrapper {

		private PooledConnection(Connection conn) {
			super(conn);
		}

		@Override
		public void close() throws SQLException {
			// release to pool
			release(getWrapped());
		}
	}

}
