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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import at.molindo.dbcopy.Insertable;
import at.molindo.dbcopy.operation.Delete;
import at.molindo.dbcopy.operation.Insert;
import at.molindo.dbcopy.operation.Operation;
import at.molindo.dbcopy.operation.Update;
import at.molindo.dbcopy.util.Utils;

public class BatchWriter extends AbstractConnectionRunnable {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BatchWriter.class);

	private final Insertable _table;
	private final BlockingQueue<Operation> _queue;
	private final int _bulkSize;

	private final List<Insert> _insertBuffer;

	public BatchWriter(Insertable table, BlockingQueue<Operation> queue) {
		if (table == null) {
			throw new NullPointerException("table");
		}
		if (queue == null) {
			throw new NullPointerException("queue");
		}
		_table = table;
		_queue = queue;
		_bulkSize = table.getBulkSize();

		_insertBuffer = new ArrayList<Insert>(table.getBulkSize());
	}

	@Override
	protected void run(Connection connection) throws SQLException {

		PreparedStatement insert = connection.prepareStatement(_table.getInsertQuery());
		PreparedStatement bulkInsert = connection.prepareStatement(_table.getBulkInsertQuery());
		PreparedStatement update = connection.prepareStatement(_table.getUpdateQuery());
		PreparedStatement delete = connection.prepareStatement(_table.getDeleteQuery());

		try {
			Operation op;
			int updatesAdded = 0, deletesAdded = 0;
			while ((op = _queue.take()) != Operation.END) {

				if (op instanceof Insert) {
					_insertBuffer.add((Insert) op);

					if (_insertBuffer.size() % _bulkSize == 0) {
						executeInserts(insert, bulkInsert, _insertBuffer);
					}
				} else if (op instanceof Update) {
					_table.update(update, op.getValues());
					update.addBatch();
					update.clearParameters();
					updatesAdded++;

					if (updatesAdded % _bulkSize == 0) {
						executeBatch(update, updatesAdded);
						updatesAdded = 0;
					}
				} else if (op instanceof Delete) {
					_table.delete(delete, op.getValues());
					delete.addBatch();
					delete.clearParameters();
					deletesAdded++;

					if (updatesAdded % _bulkSize == 0) {
						executeBatch(delete, deletesAdded);
						deletesAdded = 0;
					}
				}
			}
			executeInserts(insert, bulkInsert, _insertBuffer);
			executeBatch(update, updatesAdded);
			executeBatch(delete, deletesAdded);
		} catch (SQLException e) {
			log.warn("shutting down BatchWriter for table " + _table.getName() + " after error");
			throw e;
		} catch (InterruptedException e) {
			log.warn("batch writer interrupted");
		} finally {
			Utils.close(insert);
			Utils.close(bulkInsert);
			Utils.close(update);
			Utils.close(delete);
		}
	}

	private void executeBatch(PreparedStatement ps, int expected) throws SQLException {
		if (expected == 0) {
			// nothing to do
			return;
		}

		int[] updateCounts = ps.executeBatch();

		if (updateCounts.length != expected) {
			log.warn("unexpected number of return statuses (" + updateCounts.length + " but expected " + expected + ")");
		}

		int notModified = 0, modified = 0, failed = 0;
		for (int i = 0; i < updateCounts.length; i++) {
			int updateCount = updateCounts[i];
			if (updateCount == 0) {
				notModified++;
			} else if (updateCount == 1) {
				modified++;
			} else {
				failed++;
			}
		}

		// logging
		if (log.isDebugEnabled()) {
			String msg = "batch update executed on " + _table.getName() + " (" + modified + " modified, " + notModified
					+ " not modified, " + failed + " failed)";
			if (failed > 0) {
				log.warn(msg);
			} else {
				log.debug(msg);
			}
		} else if (failed > 0) {
			log.warn("batch updated execution failed for " + failed + " updates");
		}
	}

	private void executeInserts(PreparedStatement batch, PreparedStatement bulk, List<Insert> buffer)
			throws SQLException {

		try {
			if (buffer.size() != _bulkSize) {
				// fallback to batch
				for (Insert i : buffer) {
					_table.insert(batch, i.getValues());
					batch.addBatch();
					batch.clearParameters();
				}
				executeBatch(batch, buffer.size());
			} else {
				// real bulk
				int bulkPosition = 0;
				for (Insert i : buffer) {
					_table.insert(bulk, i.getValues(), bulkPosition++);
				}

				int inserts = bulk.executeUpdate();
				bulk.clearParameters();

				if (inserts != buffer.size()) {
					log.warn("bulk insert only inserted " + inserts + " of " + buffer.size() + " rows into "
							+ _table.getName());
				} else if (log.isDebugEnabled()) {
					log.debug("bulk insert inserted " + inserts + " rows into " + _table.getName());
				}
			}
		} finally {
			buffer.clear();
		}
	}
}