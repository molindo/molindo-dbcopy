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
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

import at.molindo.dbcopy.Insertable;
import at.molindo.dbcopy.operation.Delete;
import at.molindo.dbcopy.operation.Insert;
import at.molindo.dbcopy.operation.Operation;
import at.molindo.dbcopy.operation.Update;
import at.molindo.dbcopy.util.Utils;

/**
 * A {@link Runnable} that writes entries from a {@link BlockingQueue} to the
 * log until it receives {@link Utils#END}.
 */
public class DryWriter extends AbstractConnectionRunnable {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DryWriter.class);

	private final Insertable _table;
	private final BlockingQueue<Operation> _queue;

	public DryWriter(Insertable table, BlockingQueue<Operation> queue) {
		if (table == null) {
			throw new NullPointerException("table");
		}
		if (queue == null) {
			throw new NullPointerException("queue");
		}
		_table = table;
		_queue = queue;
	}

	@Override
	protected void run(Connection connection) throws SQLException {
		try {

			int[] indexes = _table.getComparator().getIndexes();
			Object[] values = new Object[indexes.length];

			Operation op;
			int inserts = 0, updates = 0, deletes = 0;

			String insert = "Insert " + _table.getName() + " ";
			String update = "Update " + _table.getName() + " ";
			String delete = "Delete " + _table.getName() + " ";

			while ((op = _queue.take()) != Operation.END) {

				if (log.isDebugEnabled()) {
					Object[] row = op.getValues();
					for (int i = 0; i < values.length; i++) {
						values[i] = row[indexes[i]];
					}
				}

				if (op instanceof Insert) {
					inserts++;
					if (log.isDebugEnabled()) {
						log.debug(insert + Arrays.toString(values));
					}
				} else if (op instanceof Update) {
					updates++;
					if (log.isDebugEnabled()) {
						log.debug(update + Arrays.toString(values));
					}
				} else if (op instanceof Delete) {
					deletes++;
					if (log.isDebugEnabled()) {
						log.debug(delete + Arrays.toString(values));
					}
				}
			}
			log.info("finished dry writing with " + inserts + " inserts, " + updates + " updates, " + deletes
					+ " deletes");
		} catch (InterruptedException e) {
			log.warn("batch writer interrupted", e);
		}
	}

}
