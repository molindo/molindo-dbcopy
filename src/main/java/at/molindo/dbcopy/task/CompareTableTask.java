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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import at.molindo.dbcopy.Column;
import at.molindo.dbcopy.Database;
import at.molindo.dbcopy.Insertable;
import at.molindo.dbcopy.Selectable;
import at.molindo.dbcopy.operation.Delete;
import at.molindo.dbcopy.operation.Insert;
import at.molindo.dbcopy.operation.Operation;
import at.molindo.dbcopy.operation.Update;
import at.molindo.dbcopy.util.DbcopyProperties;
import at.molindo.dbcopy.util.Equals;
import at.molindo.dbcopy.util.NaturalRowComparator;
import at.molindo.dbcopy.util.Utils;

/**
 * A {@link Runnable} implementation that compares rows retrieved in natural
 * order from a {@link Selectable} and an {@link Insertable} (using
 * {@link SelectReader}s) and submits {@link Operation}s to a {@link DryWriter}
 * (dryRun=true) or a {@link BatchWriter} (dryRun=false)
 */
public class CompareTableTask implements Runnable {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CompareTableTask.class);

	private static final boolean CHECK_ORDER = true;
	private static final boolean FAIL_ON_WRONG_ORDER = true;

	private final Selectable _sourceSelectable;
	private final Insertable _targetInsertable;
	private final Database _source;
	private final Database _target;
	private final boolean _dryRun;

	private final String _description;

	public CompareTableTask(String tableName, Database source, Database target, DbcopyProperties props) {
		this(source.getTable(tableName), tableName, source, target, props);
	}

	public CompareTableTask(Selectable sourceSelectable, String targetTableName, Database source, Database target,
			DbcopyProperties props) {
		this(sourceSelectable, target.getTable(targetTableName), source, target, props);
	}

	public CompareTableTask(Selectable sourceSelectable, Insertable targetInsertable, Database source, Database target,
			DbcopyProperties props) {

		if (sourceSelectable == null) {
			throw new NullPointerException("sourceSelectable");
		}
		if (targetInsertable == null) {
			throw new NullPointerException("targetInsertable");
		}
		if (source == null) {
			throw new NullPointerException("source");
		}
		if (target == null) {
			throw new NullPointerException("target");
		}
		if (props == null) {
			throw new NullPointerException("props");
		}

		_sourceSelectable = sourceSelectable;
		_targetInsertable = targetInsertable;
		_source = source;
		_target = target;
		_dryRun = props.isDryRun();

		// TODO improve description
		_description = _sourceSelectable.getName() + " with " + _targetInsertable.getName();
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		log.info("comparing " + _description);

		// sourceQ contains rows from source
		BlockingQueue<Object[]> sourceQ = new ArrayBlockingQueue<Object[]>(1000);
		_source.execute(new SelectReader(_sourceSelectable, sourceQ));

		// targetQ contains rows from target
		BlockingQueue<Object[]> targetQ = new ArrayBlockingQueue<Object[]>(1000);
		_target.execute(new SelectReader(_targetInsertable, targetQ));

		// writeQ takes operations on target
		BlockingQueue<Operation> writeQ = new ArrayBlockingQueue<Operation>(1000);
		Future<?> writeFuture = _target.submit(_dryRun ? new DryWriter(_targetInsertable, writeQ) : new BatchWriter(
				_targetInsertable, writeQ));

		// comparators for both tables must be equal!
		NaturalRowComparator comp = _targetInsertable.getComparator();
		int rows = 0;
		int writes = 0;
		try {
			Object[] headerT = targetQ.take();
			Object[] headerS = sourceQ.take();

			if (headerT.length != headerS.length) {
				throw new IllegalStateException("result sets of different size when comparing " + _description
						+ " (target=" + headerT.length + ", source=" + headerS.length + ")");
			}

			for (int i = 0; i < headerT.length; i++) {
				Column th = (Column) headerT[i];
				Column sh = (Column) headerS[i];

				if (!th.getName().equals(sh.getName())) {
					throw new IllegalStateException(
							"column labels of source and target colunn must be equal when comparing " + _description
									+ " (target=" + th.getName() + ", source=" + sh.getName() + ")");
				}
			}

			Equals e = new Equals(headerT, headerS);

			Object[] t = targetQ.take();
			Object[] s = sourceQ.take();

			while (t != Utils.END || s != Utils.END) {
				rows++;
				int cmp = comp.compare(t, s);
				if (cmp == 0) {
					if (!e.equals(t, s)) {
						// update
						writeQ.put(new Update(s, t));
						writes++;
					}
					t = take(targetQ, t, comp);
					s = take(sourceQ, s, comp);
				} else if (cmp < 0) {

					// t not in source
					writeQ.put(new Delete(t));
					writes++;

					t = take(targetQ, t, comp);
				} else if (cmp > 0) {

					// s not in target
					writeQ.put(new Insert(s));
					writes++;

					s = take(sourceQ, s, comp);
				}
				if (rows % 100000 == 0 && log.isDebugEnabled()) {
					int perSecond = (int) (rows / ((System.currentTimeMillis() - start) / 1000.0));
					log.debug("compared " + rows + " rows (" + perSecond + " rows/second, " + writes
							+ " changes) from " + _description);
				}
			}
		} catch (InterruptedException e) {
			log.info("comparing " + _description + " interrupted");
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
		log.info("finished comparing " + rows + " rows (" + perSecond + " rows/second, " + writes + " changes) from "
				+ _description);
	}

	private Object[] take(BlockingQueue<Object[]> queue, Object[] prev, NaturalRowComparator comp)
			throws InterruptedException {
		Object[] next = queue.take();
		if (CHECK_ORDER) {
			int cmp = comp.compare(prev, next);
			if (cmp >= 0) {
				String msg = "unexpected order of rows for tabels " + _sourceSelectable.getName() + " and "
						+ _targetInsertable.getName() + " (" + cmp + ")";
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
