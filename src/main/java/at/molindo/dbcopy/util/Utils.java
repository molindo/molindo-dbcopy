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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Queue;

import org.apache.commons.dbutils.ResultSetHandler;

public final class Utils {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Utils.class);

	/**
	 * indicates the end of data in a {@link Queue}
	 */
	public static final Object[] END = new Object[0];

	private Utils() {
	}

	public static void execute(Connection c, String query) throws SQLException {
		execute(c, query, null);
	}

	public static <T> T execute(Connection c, String query, ResultSetHandler<T> handler) throws SQLException {
		Statement stmt = c.createStatement();
		try {
			ResultSet rs = stmt.executeQuery(query);
			return handle(rs, handler);
		} finally {
			stmt.close();
		}
	}

	public static <T> T executePrepared(Connection c, String query, ResultSetHandler<T> handler, Object... params)
			throws SQLException {
		PreparedStatement stmt = c.prepareStatement(query);
		try {
			for (int i = 0; i < params.length; i++) {
				stmt.setObject(i + 1, params[i]);
			}
			ResultSet rs = stmt.executeQuery();
			return handle(rs, handler);
		} finally {
			stmt.close();
		}
	}

	public static <T> T handle(ResultSet rs, ResultSetHandler<T> handler) throws SQLException {
		try {
			return handler == null ? null : handler.handle(rs);
		} finally {
			rs.close();
		}
	}

	public static void close(Connection c) {
		if (c != null) {
			try {
				c.close();
			} catch (SQLException e) {
				log.warn("failed to close connection");
			}
		}
	}

	public static void close(Statement ps) {
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				log.warn("failed to close statement");
			}
		}
	}

	public static void close(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				log.warn("failed to close resultset");
			}
		}
	}

	public static boolean isStringType(int dataType) {
		switch (dataType) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return true;
		default:
			return false;
		}
	}

	public static void printRow(ResultSet pk) throws SQLException {
		System.out.println("--- " + pk.getRow() + ". row ---");
		for (int i = 1; i <= pk.getMetaData().getColumnCount(); i++) {
			System.out.println(pk.getMetaData().getColumnName(i) + ": " + pk.getObject(i));
		}
	}
}
