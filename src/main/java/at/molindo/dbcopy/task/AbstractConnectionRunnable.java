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
package at.molindo.dbcopy.task;

import java.sql.Connection;
import java.sql.SQLException;


public abstract class AbstractConnectionRunnable implements ConnectionRunnable {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AbstractConnectionRunnable.class);

	private Connection _connection;

	@Override
	public final void setConnection(Connection c) {
		if (_connection != null) {
			throw new IllegalStateException("connection already set");
		}
		_connection = c;
	}

	@Override
	public final Connection unsetConnection() {
		if (_connection == null) {
			throw new IllegalStateException("no connection set");
		}
		Connection c = _connection;
		_connection = null;
		return c;
	}

	@Override
	public final void run() {
		if (_connection == null) {
			throw new IllegalStateException("no connection set");
		}
		try {
			run(_connection);
		} catch (SQLException e) {
			log.error("task failed", e);
		}
	}

	protected abstract void run(Connection connection) throws SQLException;

}