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

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import at.molindo.dbcopy.Database;
import at.molindo.dbcopy.Table;
import at.molindo.dbcopy.operation.Delete;
import at.molindo.dbcopy.operation.Insert;
import at.molindo.dbcopy.operation.Operation;
import at.molindo.dbcopy.operation.Update;
import at.molindo.dbcopy.util.NaturalRowComparator;

public class CompareTableTask implements Runnable {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CompareTableTask.class);

	private static final boolean CHECK_ORDER = true;
	private static final boolean FAIL_ON_WRONG_ORDER = true;

	private final Table _sourceTable;
	private final String _targetTableName;
	private final Database _source;
	private final Database _target;

	public CompareTableTask(String tableName, Database source, Database target) {
		this(tableName, tableName, source, target);
	}

	public CompareTableTask(String sourceTableName, String targetTableName, Database source, Database target) {
		_sourceTable = source.getTable(sourceTableName);
		_targetTableName = targetTableName;
		_source = source;
		_target = target;
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		log.info("comparing table " + _sourceTable.getName() + " with " + _targetTableName);

		if (!_target.getTableNames().contains(_targetTableName)) {
			// TODO create table
			throw new IllegalArgumentException("target table " + _targetTableName + " does not exist");
		}

		Table targetTable = _target.getTable(_targetTableName);

		// sourceQ contains rows from source
		BlockingQueue<Object[]> sourceQ = new ArrayBlockingQueue<Object[]>(1000);
		_source.execute(new TableReader(_sourceTable, sourceQ));

		// targetQ contains rows from target
		BlockingQueue<Object[]> targetQ = new ArrayBlockingQueue<Object[]>(1000);
		_target.execute(new TableReader(targetTable, targetQ));

		// writeQ takes operations on target
		BlockingQueue<Operation> writeQ = new ArrayBlockingQueue<Operation>(1000);
		Future<?> writeFuture = _target.submit(new BatchWriter(targetTable, writeQ));

		// comparators for both tables must be equal!
		NaturalRowComparator comp = _sourceTable.getComparator();
		int rows = 0;
		try {
			Object[] t = targetQ.take();
			Object[] s = sourceQ.take();

			while (t.length != 0 || s.length != 0) {
				rows++;
				int cmp = comp.compare(t, s);
				if (cmp == 0) {
					if (!Arrays.equals(t, s)) {
						// update
						writeQ.put(new Update(s, t));
					}
					t = take(targetQ, t, comp);
					s = take(sourceQ, s, comp);
				} else if (cmp < 0) {
					// t not in source
					writeQ.put(new Delete(t));
					t = take(targetQ, t, comp);
				} else if (cmp > 0) {
					// s not in target
					writeQ.put(new Insert(s));
					s = take(sourceQ, s, comp);
				}
				if (rows % 100000 == 0 && log.isDebugEnabled()) {
					int perSecond = (int) (rows / ((System.currentTimeMillis() - start) / 1000.0));
					log.debug("compared " + rows + " rows (" + perSecond + " per second) from "
							+ _sourceTable.getName() + " with " + targetTable.getName());
				}
			}
		} catch (InterruptedException e) {
			log.info("comparing " + _sourceTable.getName() + " with " + targetTable.getName() + " interrupted");
		} finally {
			try {
				writeQ.put(Operation.END);
				writeFuture.get();
			} catch (InterruptedException e) {
				log.info("signaling end to writer interrupted");
			} catch (ExecutionException e) {
				throw new RuntimeException("writer failed", e);
			}
		}

		int perSecond = (int) (rows / ((System.currentTimeMillis() - start) / 1000.0));
		log.info("finished comparing " + rows + " rows (" + perSecond + " per second) from " + _sourceTable.getName()
				+ " with " + targetTable.getName());
	}

	private Object[] take(BlockingQueue<Object[]> queue, Object[] prev, NaturalRowComparator comp)
			throws InterruptedException {
		Object[] next = queue.take();
		if (CHECK_ORDER) {
			int cmp = comp.compare(prev, next);
			if (cmp >= 0) {
				String msg = "unexpected order of rows for tabels " + _sourceTable.getName() + " and "
						+ _targetTableName + " (" + cmp + ")";
				if (FAIL_ON_WRONG_ORDER) {
					throw new RuntimeException(msg);
				} else {
					log.warn(msg);
				}
			}
		}
		return next;
	}

}
