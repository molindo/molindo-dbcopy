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

import static at.molindo.utils.collections.IteratorUtils.transform;
import static at.molindo.utils.data.StringUtils.join;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import at.molindo.dbcopy.util.SqlFunction;
import at.molindo.utils.collections.ArrayUtils;
import at.molindo.utils.collections.CollectionUtils;
import at.molindo.utils.data.Function;

public class SynchronizedReadOnlyConnectionPool extends DefaultConnectionPool implements DataSource {

	private String[] _synchonized;

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(SynchronizedReadOnlyConnectionPool.class);

	private static String lockTableQuery(String[] tables) {

		List<String> sortedTables = CollectionUtils.list(tables);
		Collections.sort(sortedTables);

		return "LOCK TABLES " + join(", ", transform(sortedTables, new Function<String, String>() {
			@Override
			public String apply(String table) {
				return "`" + table + "` READ";
			}
		}));
	}

	public SynchronizedReadOnlyConnectionPool(String name, String jdbcUrl, String user, String password, int poolSize) {
		super(name, jdbcUrl, user, password, poolSize);
	}

	/**
	 * sync all connections for consistent reading
	 * 
	 * <ol> <li>lock all given tables for reading</li> <li>start a transaction
	 * in {@link Connection#TRANSACTION_REPEATABLE_READ}</li> <li>unlock
	 * tables</li> </ol>
	 */
	public synchronized void syncRead(String... tables) {
		if (ArrayUtils.empty(tables)) {
			throw new IllegalArgumentException("no tables to synchonize");
		}
		if (_synchonized != null) {
			throw new IllegalStateException("already synchonized");
		}
		assertIdle();

		// lock tables
		executeAll(lockTableQuery(tables));
		// start transaction
		each(new SqlFunction<Void>() {

			@Override
			public Void apply(Connection c) throws SQLException {
				// start read-only transaction
				c.setReadOnly(true);
				c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
				c.setAutoCommit(false);
				return null;
			}
		});
		// unlock tables
		executeAll("UNLOCK TABLES");

		_synchonized = tables;

		if (log.isDebugEnabled()) {
			log.debug("synchronized data source tables: " + Arrays.asList(_synchonized));
		}
	}

	public synchronized void unsyncRead() {
		if (_synchonized == null) {
			throw new IllegalStateException("not synchronized");
		}
		assertIdle();

		// end transaction
		each(new SqlFunction<Void>() {

			@Override
			public Void apply(Connection c) throws SQLException {
				c.rollback();
				return null;
			}
		});

		if (log.isDebugEnabled()) {
			log.debug("un-synchronized data source tables: " + Arrays.asList(_synchonized));
		}

		_synchonized = null;
	}

	protected void onClose() {
		if (_synchonized != null) {
			unsyncRead();
		}
	}

}
