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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ConnectionWrapper implements Connection {

	private final Connection _wrapped;

	public ConnectionWrapper(Connection wrapper) {
		this._wrapped = wrapper;
	}

	public final Connection getWrapped() {
		return _wrapped;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return _wrapped.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return _wrapped.isWrapperFor(iface);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return _wrapped.createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return _wrapped.prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return _wrapped.prepareCall(sql);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return _wrapped.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		_wrapped.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return _wrapped.getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		_wrapped.commit();
	}

	@Override
	public void rollback() throws SQLException {
		_wrapped.rollback();
	}

	@Override
	public void close() throws SQLException {
		_wrapped.close();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return _wrapped.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return _wrapped.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		_wrapped.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return _wrapped.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		_wrapped.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return _wrapped.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		_wrapped.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return _wrapped.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return _wrapped.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		_wrapped.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return _wrapped.createStatement(resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return _wrapped.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return _wrapped.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return _wrapped.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		_wrapped.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		_wrapped.setHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		return _wrapped.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return _wrapped.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return _wrapped.setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		_wrapped.rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		_wrapped.releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return _wrapped.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return _wrapped.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return _wrapped.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return _wrapped.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return _wrapped.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return _wrapped.prepareStatement(sql, columnNames);
	}

	@Override
	public Clob createClob() throws SQLException {
		return _wrapped.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return _wrapped.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return _wrapped.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return _wrapped.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return _wrapped.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		_wrapped.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		_wrapped.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return _wrapped.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return _wrapped.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return _wrapped.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return _wrapped.createStruct(typeName, attributes);
	}

	// Java7 additions

	@Override
	public void setSchema(String schema) throws SQLException {
		_wrapped.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		return _wrapped.getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		_wrapped.abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		_wrapped.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return _wrapped.getNetworkTimeout();
	}

}
