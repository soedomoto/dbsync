package com.soedomoto.dbsync.api.dbms;

import java.sql.SQLException;
import java.util.List;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

@ThriftService
public interface DBMS {
	@ThriftMethod
	public List<String> getTables() throws SQLException;
	@ThriftMethod
	public List<Column> getColumns(String table) throws SQLException;
	@ThriftMethod
	public List<String> getPrimaryKeys(String table) throws SQLException;
	@ThriftMethod
	public List<String> getIndices(String table) throws SQLException;
	@ThriftMethod
	public List<List<byte[]>> getRows(String table, int offset, int limit) throws SQLException;
	@ThriftMethod
	public Integer getCurrentOffset(String table) throws SQLException;
	@ThriftMethod
	public String getUrl();
}
