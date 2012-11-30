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
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import at.molindo.dbcopy.source.DataSource;
import at.molindo.dbcopy.util.ConnectionWrapper;
import at.molindo.dbcopy.util.NamedThreadFactory;
import at.molindo.dbcopy.util.Utils;

/**
 * {@link ThreadPoolExecutor} using {@link DataSourceThreadFactory} which gets
 * close on termination
 */
public class ConnectionExecutorService extends ThreadPoolExecutor {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ConnectionExecutorService.class);

	private final LinkedBlockingQueue<ThreadPoolConnection> _connections = new LinkedBlockingQueue<ConnectionExecutorService.ThreadPoolConnection>();

	private final String _name;
	private final int _poolSize;

	public ConnectionExecutorService(int poolSize, DataSource dataSource) {
		super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory("pool-" + dataSource.getName() + "-"));

		_name = dataSource.getName();
		_poolSize = poolSize;

		for (int i = 0; i < poolSize; i++) {
			// get all connections needed for pool
			_connections.add(new ThreadPoolConnection(dataSource.getConnection()));
		}

		log.debug("started executor service for " + _name);
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		return new ConnectionFutureTask<T>(runnable, value);
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
		return new ConnectionFutureTask<T>(callable);
	}

	@Override
	protected void terminated() {
		if (_connections.size() != _poolSize) {
			throw new IllegalStateException("no all connections returned");
		}

		for (ThreadPoolConnection c : _connections) {
			Utils.close(c.getWrapped());
		}
		super.terminated();
		log.debug("terminated executor service for " + _name);
	}

	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		super.beforeExecute(t, r);
		if (r instanceof ConnectionRunnable) {
			Connection c = _connections.poll();
			if (c == null) {
				throw new IllegalStateException("no connections remaining for " + _name);
			}
			((ConnectionRunnable) r).setConnection(c);
		} else if (log.isTraceEnabled()) {
			log.trace("not a ConnectionRunnable: " + r.getClass().getName());
		}
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		if (r instanceof ConnectionRunnable) {
			ThreadPoolConnection c = (ThreadPoolConnection) ((ConnectionRunnable) r).unsetConnection();
			if (_connections.contains(c)) {
				throw new IllegalStateException("connection already in pool");
			}
			_connections.add(c);
		} else if (log.isTraceEnabled()) {
			log.trace("not a ConnectionRunnable: " + r.getClass().getName());
		}
		super.afterExecute(r, t);
	}

	private static class ThreadPoolConnection extends ConnectionWrapper {

		private ThreadPoolConnection(Connection conn) {
			super(conn);
		}

		@Override
		public void close() throws SQLException {
			log.warn("a task must not close the connection");
		}
	}

	private static class ConnectionFutureTask<T> extends FutureTask<T> implements ConnectionRunnable {

		private ConnectionRunnable _wrapped;
		private Connection _connection;

		public ConnectionFutureTask(Runnable runnable, T result) {
			super(runnable, result);
			if (runnable instanceof ConnectionRunnable) {
				_wrapped = (ConnectionRunnable) runnable;
			}
		}

		public ConnectionFutureTask(Callable<T> callable) {
			super(callable);
			if (callable instanceof ConnectionRunnable) {
				_wrapped = (ConnectionRunnable) callable;
			}
		}

		@Override
		public void setConnection(Connection c) {
			if (_wrapped != null) {
				_wrapped.setConnection(c);
			} else if (_connection != null) {
				throw new IllegalStateException("connection already set");
			} else {
				_connection = c;
			}
		}

		@Override
		public Connection unsetConnection() {
			if (_wrapped != null) {
				return _wrapped.unsetConnection();
			} else if (_connection == null) {
				throw new IllegalStateException("no connection set");
			} else {
				Connection c = _connection;
				_connection = null;
				return c;
			}
		}
	}
}
