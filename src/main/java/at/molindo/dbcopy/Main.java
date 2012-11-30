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
package at.molindo.dbcopy;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import at.molindo.dbcopy.task.CompareTableTask;
import at.molindo.dbcopy.util.DbcopyProperties;
import at.molindo.dbcopy.util.DbcopyProperties.DbProperties;
import at.molindo.dbcopy.util.DbcopyProperties.TableTaksProperties;
import at.molindo.mysqlcollations.lib.CollationComparator;
import at.molindo.utils.data.StringUtils;
import at.molindo.utils.properties.SystemProperty;

public class Main {

	static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Main.class);

	static {
		File tmpDir = new File(SystemProperty.JAVA_IO_TMPDIR.getFile(), Main.class.getPackage().getName());
		if (!tmpDir.isDirectory()) {
			if (tmpDir.exists()) {
				throw new IllegalStateException("temporary directory exists but isn't a directory: "
						+ tmpDir.getAbsolutePath());
			} else if (!tmpDir.mkdirs()) {
				throw new IllegalStateException("can't create temporary directory: " + tmpDir.getAbsolutePath());
			}
		}
		CollationComparator.setLibraryDirectoryName(tmpDir.getAbsolutePath());
	}

	private static Set<String> getTableNames(Database source, Database target, TableTaksProperties props) {

		Set<String> available = source.getTableNames();
		available.retainAll(target.getTableNames());

		log.info("available tables: " + available);

		Set<String> tables = new HashSet<String>();

		Set<String> includes = props.getIncludes();
		if (!includes.isEmpty()) {
			// excludes first, then includes
			for (String include : includes) {
				if (include.endsWith("*")) {
					include = StringUtils.beforeLast(include, "*");
					for (String t : available) {
						if (t.startsWith(include)) {
							if (log.isTraceEnabled()) {
								log.trace("including " + t);
							}
							tables.add(t);
						}
					}
				} else {
					if (log.isTraceEnabled()) {
						log.trace("including " + include);
					}
					tables.add(include);
				}
			}
		} else {
			// add all, then excludes
			if (log.isTraceEnabled()) {
				log.trace("including all tables");
			}
			tables.addAll(available);
		}

		// process excludes
		for (String exclude : props.getExcludes()) {
			if (exclude.endsWith("*")) {
				exclude = StringUtils.beforeLast(exclude, "*");

				Iterator<String> iter = tables.iterator();
				while (iter.hasNext()) {
					String t = iter.next();
					if (t.startsWith(exclude)) {
						if (log.isTraceEnabled()) {
							log.trace("excluding " + t);
						}
						iter.remove();
					}
				}
			} else {
				if (log.isTraceEnabled()) {
					log.trace("excluding " + exclude);
				}
				tables.remove(exclude);
			}
		}

		log.info("creating tasks for tables: " + tables);

		return tables;
	}

	private static Database open(DbProperties props) {
		return new Database(props.getRole(), props.getJdbcUrl(), props.getUser(), props.getPassword(),
				props.getPoolSize());
	}

	public static void main(String[] args) {

		DbcopyProperties props = DbcopyProperties.load(args.length > 0 ? args[0] : null);

		Database source = open(props.getSource());
		Database target = open(props.getTarget());

		// make sure variables are equal
		Map<String, Object> variables = source.getVariables("character_set_client", "character_set_connection",
				"character_set_results", "collation_connection");

		// set some target only variables
		variables.put("foreign_key_checks", 0);
		if (props.isDisableUniqueChecks()) {
			variables.put("unique_checks", 0);
		}

		target.setVariables(variables);

		source.start();
		target.start();

		ExecutorService executor = Executors.newFixedThreadPool(props.getSource().getPoolSize());

		Set<String> tables = getTableNames(source, target, props.getTableTask());

		for (String table : tables) {
			executor.execute(new CompareTableTask(table, source, target));
		}

		executor.shutdown();
		try {
			executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.info("waiting for terminatino of executor interrupted");
		}

		log.info("finished tasks, shutting down");

		target.close();
		source.close();
	}

}
