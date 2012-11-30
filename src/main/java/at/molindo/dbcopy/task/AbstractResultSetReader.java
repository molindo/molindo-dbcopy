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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;

import at.molindo.dbcopy.util.Utils;

public abstract class AbstractResultSetReader extends AbstractConnectionRunnable {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AbstractResultSetReader.class);

	private final BlockingQueue<Object[]> _queue;

	public AbstractResultSetReader(BlockingQueue<Object[]> queue) {
		if (queue == null) {
			throw new NullPointerException("queue");
		}
		_queue = queue;
	}

	@Override
	protected void run(Connection connection) throws SQLException {

		ResultSet res = executeQuery(connection);

		try {
			java.sql.ResultSetMetaData meta = res.getMetaData();

			int columns = meta.getColumnCount();

			while (res.next()) {
				Object[] row = new Object[columns];
				for (int i = 0; i < columns; i++) {
					row[i] = res.getObject(i + 1);
				}
				try {
					_queue.put(row);
				} catch (InterruptedException e) {
					log.info("reading '" + getQuery() + "' interrupted");
					break;
				}
			}

			try {
				_queue.put(Utils.END);
			} catch (InterruptedException e) {
				log.info("finishing '" + getQuery() + "' interrupted");
			}
		} finally {
			res.close();
		}
	}

	protected ResultSet executeQuery(Connection connection) throws SQLException {
		// streaming resultset
		Statement stmt = connection.prepareStatement(getQuery(), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		return stmt.executeQuery(getQuery());
	}

	/**
	 * @return debugging only, may be ""
	 */
	protected abstract String getQuery();
}