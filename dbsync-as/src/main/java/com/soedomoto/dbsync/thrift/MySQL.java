package com.soedomoto.dbsync.thrift;

import java.net.SocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.soedomoto.dbsync.service.DBSyncDaemon;
import com.soedomoto.dbsync.thrift.Column;
import com.soedomoto.dbsync.thrift.DBMS;

public class MySQL implements DBMS, DBSyncDaemon.Client {
	private static Logger log = LoggerFactory.getLogger(MySQL.class);
	
	private String url;
	private Connection connection;
	private Map<SocketAddress, Map<String, Integer>> offset = new HashMap<>();
	private SocketAddress currentClient;

	public MySQL(String url, String username, String password) throws ClassNotFoundException, SQLException {
		this.url = url;
		
		Class.forName("com.mysql.jdbc.Driver");
		this.connection = DriverManager.getConnection(url, username, password);
	}
	
	@Override
	public List<String> getTables() throws SQLException {
		String sql = String.format("SELECT DISTINCT TABLE_NAME "
					+ "FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '%s'", connection.getCatalog());
		
		List<String> tables = new ArrayList<>();
		ResultSet rs = connection.prepareStatement(sql).executeQuery();
		while(rs.next()) {
			String name = rs.getString("TABLE_NAME");
			tables.add(name);
		}
		
		log.info(String.format("Send tables : %s", Arrays.toString(tables.toArray(new String[]{}))));
		
		return tables;
	}

	@Override
	public List<Column> getColumns(String table) throws SQLException {
		String sql = String.format("SELECT DISTINCT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT "
					+ "FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'", table);
		
		List<Column> cols = new ArrayList<>();
		ResultSet rs = connection.prepareStatement(sql).executeQuery();
		while(rs.next()) {
			String name = rs.getString("COLUMN_NAME");
			String type = rs.getString("DATA_TYPE");
			String nullable = rs.getString("IS_NULLABLE");
			String colDefault = rs.getString("COLUMN_DEFAULT");
			
			Column col = new Column(name, type, nullable.equalsIgnoreCase("no") ? false : true, colDefault);
			cols.add(col);
		}
		
		log.info("Send Columns...");
		
		return cols;
	}

	@Override
	public List<String> getPrimaryKeys(String table) throws SQLException {
		String sql = String.format("SHOW KEYS FROM %s WHERE Key_name = ?", table);
		
		List<String> pks = new ArrayList<>();
		PreparedStatement ps = connection.prepareStatement(sql);
		ps.setString(1, "PRIMARY");
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
			String name = rs.getString("Column_name");
			pks.add(name);
		}
		
		log.info(String.format("Send Primary Keys : %s", Arrays.toString(pks.toArray(new String[]{}))));
		
		return pks;
	}

	@Override
	public List<String> getIndices(String table) throws SQLException {
		String sql = String.format("SELECT DISTINCT INDEX_NAME "
					+ "FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?");
		
		PreparedStatement ps = connection.prepareStatement(sql);
		ps.setString(1, connection.getCatalog());
		ps.setString(2, table);
		
		List<String> indices = new ArrayList<>();
		indices.addAll(getPrimaryKeys(table));
		
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
			String name = rs.getString("INDEX_NAME");
			if(name.equalsIgnoreCase("primary")) continue;
			
			indices.add(name);
		}
		
		log.info(String.format("Send Indices : %s", Arrays.toString(indices.toArray(new String[]{}))));
		
		return indices;
	}

	@Override
	public List<List<byte[]>> getRows(String table, int offset, int limit) throws SQLException {
		getCurrentOffset(table);
		this.offset.get(currentClient).put(table, offset);
		
		int colCount = getColumns(table).size();
		
		log.info(String.format("Query : %s", String.format("SELECT * FROM %s LIMIT %s , %s", table, offset, limit)));
		PreparedStatement ps = connection.prepareStatement(String.format("SELECT * FROM %s LIMIT %s , %s", table, offset, limit));
		ResultSet rs = ps.executeQuery();
		
		List<List<byte[]>> rows = new ArrayList<>();
		while(rs.next()) {
			List<byte[]> row = new ArrayList<>();
			for(int c=1; c<=colCount; c++) {
				byte[] val = rs.getBytes(c);
				if(rs.wasNull()) val = new byte[0];
				
				row.add(val);
			}
			
			rows.add(row);
		}
		
		this.offset.get(currentClient).put(table, offset + limit);
		return rows;
	}

	@Override
	public Integer getCurrentOffset(String table) throws SQLException {
		if(offset.get(currentClient) == null) {
			offset.put(currentClient, new HashMap<String, Integer>());
		} 
		
		if(offset.get(currentClient).get(table) == null) {
			offset.get(currentClient).put(table, 0);
		}
		
		return offset.get(currentClient).get(table);
	}

	@Override
	public String getUrl() {
		return url;
	}

	@Override
	public void setCurrentAddress(SocketAddress addr) {
		this.currentClient = addr;
	}
	
}
