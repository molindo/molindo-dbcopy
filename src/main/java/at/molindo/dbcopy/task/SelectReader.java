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
import java.util.Date;
import java.util.concurrent.BlockingQueue;

import at.molindo.dbcopy.Column;
import at.molindo.dbcopy.Selectable;
import at.molindo.dbcopy.util.Utils;

/**
 * a {@link Runnable} implementation that reads rows from a {@link Selectable}
 * (using {@link Selectable#getOrderedSelect()} and submits them to a
 * {@link BlockingQueue}, preceded by the header (an Object[] of {@link Column}
 * s) and succeeded by {@link Utils#END}. It uses a MySQL streaming resultset to
 * do so.
 */
public class SelectReader extends AbstractConnectionRunnable {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SelectReader.class);

	private final Selectable _source;
	private final BlockingQueue<Object[]> _queue;

	public SelectReader(Selectable source, BlockingQueue<Object[]> queue) {
		if (source == null) {
			throw new NullPointerException("source");
		}
		if (queue == null) {
			throw new NullPointerException("queue");
		}
		_source = source;
		_queue = queue;
	}

	@Override
	protected void run(Connection connection) throws SQLException {

		ResultSet res = executeQuery(connection);

		try {
			java.sql.ResultSetMetaData meta = res.getMetaData();
			int columns = meta.getColumnCount();

			try {
				// send header first
				Object[] header = new Object[columns];
				for (int i = 0; i < columns; i++) {
					Class<?> cls = Class.forName(meta.getColumnClassName(i + 1));

					/*
					 * workaround for getColumnClassName(..) ignoring
					 * yearIsDateType=false
					 * 
					 * TODO link or create bug report
					 */
					if (Date.class.isAssignableFrom(cls) && "YEAR".equals(meta.getColumnTypeName(i + 1))) {
						cls = Short.class;
					}

					header[i] = new Column(meta.getColumnLabel(i + 1), cls);
				}
				_queue.put(header);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("columnClassName unknown");
			}

			while (res.next()) {
				Object[] row = new Object[columns];
				for (int i = 0; i < columns; i++) {
					row[i] = res.getObject(i + 1);
				}
				_queue.put(row);
			}

			_queue.put(Utils.END);
		} catch (InterruptedException e) {
			log.info("reading '" + getQuery() + "' interrupted");
		} finally {
			Utils.close(res);
		}
	}

	protected ResultSet executeQuery(Connection connection) throws SQLException {
		/*
		 * MySQL streaming resultset:
		 * "The combination of a forward-only, read-only result set, with a fetch size of Integer.MIN_VALUE serves as a signal to the driver to stream result sets row-by-row."
		 * 
		 * http://dev.mysql.com/doc/connector-j/en/connector-j-reference-
		 * implementation-notes.html
		 */
		Statement stmt = connection.prepareStatement(getQuery(), ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		return stmt.executeQuery(getQuery());
	}

	protected String getQuery() {
		return _source.getOrderedSelect();
	}
}