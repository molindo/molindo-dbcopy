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
package at.molindo.dbcopy.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import at.molindo.dbcopy.source.DataSourceRole;
import at.molindo.utils.collections.IteratorUtils;
import at.molindo.utils.collections.IteratorWrappers;
import at.molindo.utils.data.StringUtils;

public class DbcopyProperties {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DbcopyProperties.class);

	public static final String PROPERTIES_FILE = "dbcopy.properties";
	public static final String DEFAULT_PROPERTIES_FILE = "/defaults.properties";

	private final Properties _props;

	private final DbProperties _source;
	private final DbProperties _target;
	private final TableTaksProperties _tables;
	private final QueryTaskProperties _queries;

	public static DbcopyProperties load(@Nullable String fileName) {

		Properties defaults = new Properties();
		try {
			defaults.load(DbcopyProperties.class.getResourceAsStream(DEFAULT_PROPERTIES_FILE));
		} catch (IOException e) {
			throw new RuntimeException("can't load defaults", e);
		}

		if (fileName == null) {
			fileName = PROPERTIES_FILE;
		}
		InputStream in = DbcopyProperties.class.getResourceAsStream("/" + fileName);
		if (in == null) {
			log.debug("can't find /" + fileName + " in classpath");

			// try file
			File file = new File(fileName == null ? PROPERTIES_FILE : fileName);
			try {
				in = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				log.debug("file does not exist: " + file.getAbsolutePath());
			}
		}

		if (in == null) {
			throw new IllegalArgumentException("missing properties file: " + fileName);
		}

		Properties props = new Properties(defaults);
		try {
			props.load(in);
		} catch (IOException e) {
			throw new RuntimeException("can't load properties from " + fileName, e);
		}

		return new DbcopyProperties(props);
	}

	private DbcopyProperties(Properties props) {
		if (props == null) {
			throw new NullPointerException("props");
		}
		_props = props;

		_source = new DbProperties(DataSourceRole.SOURCE);
		_target = new DbProperties(DataSourceRole.TARGET);
		_tables = new TableTaksProperties();
		_queries = new QueryTaskProperties();
	}

	public DbProperties getSource() {
		return _source;
	}

	public DbProperties getTarget() {
		return _target;
	}

	public TableTaksProperties getTableTasks() {
		return _tables;
	}

	public QueryTaskProperties getQueryTasks() {
		return _queries;
	}

	public boolean isDisableUniqueChecks() {
		return getBool("db.disable_unique_checks");
	}

	public boolean isDryRun() {
		return getBool("db.dry_run", false);
	}

	public String getString(String key) throws MissingPropertyException {
		String p = _props.getProperty(key);
		if (StringUtils.empty(p)) {
			throw new MissingPropertyException(key);
		}
		return p;
	}

	public String getString(String key, String defaultValue) {
		return _props.getProperty(key, defaultValue);
	}

	public int getInt(String key) throws MissingPropertyException {
		try {
			return Integer.parseInt(getString(key));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("can't parse integer from property value " + key, e);
		}
	}

	public boolean getBool(String key) throws MissingPropertyException {
		return Boolean.parseBoolean(getString(key));
	}

	public boolean getBool(String key, boolean defaultValue) throws MissingPropertyException {
		return Boolean.parseBoolean(getString(key, Boolean.toString(defaultValue)));
	}

	public Set<String> getSet(String key) {
		Set<String> set = new LinkedHashSet<String>();
		try {
			for (String include : IteratorWrappers.trim(StringUtils.split(getString(key), ","))) {
				if (!StringUtils.empty(include)) {
					set.add(include);
				}
			}
		} catch (MissingPropertyException e) {

		}
		return set;
	}

	public class DbProperties {

		private final String _prefix;
		private final DataSourceRole _role;

		private DbProperties(DataSourceRole role) {
			_role = role;
			_prefix = StringUtils.trailing(role.toString().toLowerCase(), ".");
		}

		public DataSourceRole getRole() {
			return _role;
		}

		public String getJdbcUrl() {
			return getString(_prefix + "jdbc");
		}

		public String getUser() {
			return getString(_prefix + "user");

		}

		public String getPassword() {
			return getString(_prefix + "pass");

		}

		public int getPoolSize() {
			return getInt(_prefix + "pool");
		}
	}

	public class TableTaksProperties {

		private final String _prefix = "task.tables.";

		public Set<String> getExcludes() {
			return getSet(_prefix + "exclude");
		}

		public Set<String> getIncludes() {
			return getSet(_prefix + "include");
		}

	}

	public class QueryTaskProperties implements Iterable<QueryTask> {

		private final LinkedHashMap<String, QueryTask> _tasks = new LinkedHashMap<String, DbcopyProperties.QueryTask>();

		private final String _prefix = "task.queries.";

		public QueryTaskProperties() {
			for (Map.Entry<Object, Object> e : _props.entrySet()) {
				String key = (String) e.getKey();
				if (key.startsWith(_prefix)) {
					String name = StringUtils.beforeFirst(key.substring(_prefix.length()), ".");
					if (!_tasks.containsKey(name)) {
						_tasks.put(name, new QueryTask(_prefix + name));
					}
				}
			}
		}

		@Override
		public Iterator<QueryTask> iterator() {
			return IteratorUtils.readOnly(_tasks.values().iterator());
		}

		public Map<String, QueryTask> getTasks() {
			return Collections.unmodifiableMap(_tasks);
		}
	}

	public class QueryTask {

		private final String _prefix;
		private final String _name;

		public QueryTask(String prefix) {
			_prefix = StringUtils.trailing(prefix, ".");
			_name = StringUtils.afterLast(_prefix.substring(0, _prefix.length() - 1), ".");
		}

		public String getName() {
			return _name;
		}

		public String getQuery() {
			return getString(_prefix + "query");
		}

		public String getTable() {
			return getString(_prefix + "table");
		}

		@CheckForNull
		public String getIndex() {
			return getString(_prefix + "index", null);
		}
	}

	public static class MissingPropertyException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public MissingPropertyException(String key) {
			super("missing property: " + key);
		}
	}

}
