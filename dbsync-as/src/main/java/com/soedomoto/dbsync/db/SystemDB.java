package com.soedomoto.dbsync.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.soedomoto.dbsync.config.DatabaseDescriptor;

public class SystemDB {
	private static final Logger log = LoggerFactory.getLogger(SystemDB.class);
	
	private final String SYSTEMDB = "dbsync";
	private final String SYSTEMUSER = "dba";
	private final String SYSTEMPASS = "";
	
	private static Connection connection;
	
	public static final SystemDB instance = new SystemDB();
	static {
		// Init system table
		try {
			Statement stmt = connection.createStatement();
			stmt.execute("CREATE TABLE IF NOT EXISTS peers " + 
						"(peer VARCHAR(15) PRIMARY KEY, token VARCHAR(255))");
			stmt.execute("CREATE TABLE IF NOT EXISTS local " + 
						"(key VARCHAR(255) PRIMARY KEY, cluster_name VARCHAR(255), data_center VARCHAR(255), " + 
						"rack VARCHAR(255), broadcast_address VARCHAR(255), listen_address VARCHAR(255))");
		} catch (SQLException e) {
			log.error("Error in initializing system table", e);
		}
	}

	private SystemDB() {
		try {
			Class.forName("org.h2.Driver");
			connection = DriverManager.getConnection("jdbc:h2:./" + SYSTEMDB, SYSTEMUSER, SYSTEMPASS);
		} catch (ClassNotFoundException e) {
			log.error("Could not load H2 database driver", e);
		} catch (SQLException e) {
			log.error("Could not connect to H2 database", e);
		}
	}
	
	public void finishStartup() {
		String sql = "INSERT INTO local (" +
                "key," +
                "cluster_name," +
                "data_center," +
                "rack," +
                "broadcast_address," +
                "listen_address" +
                ") VALUES (?, ?, ?, ?, ?, ?)";
		
		try {
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, "local");
			ps.setString(2, DatabaseDescriptor.getClusterName());
			ps.setString(3, DatabaseDescriptor.getEndpoint().getDatacenter(DatabaseDescriptor.getBroadcastAddress()));
			ps.setString(4, DatabaseDescriptor.getEndpoint().getRack(DatabaseDescriptor.getBroadcastAddress()));
			ps.setString(5, DatabaseDescriptor.getBroadcastAddress().getHostAddress());
			ps.setString(6, DatabaseDescriptor.getListenAddress().getHostAddress());
		} catch (SQLException e) {
			log.error("Error in persisting local metadata", e);
		}
	}
	
	public Map<InetAddress, String> getTokens() {
		Map<InetAddress, String> tokenMap = new HashMap<>();
		
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT peer, token FROM peers");
			while(rs.next()) {
				String peer = rs.getString("peer");
				InetAddress ip;
				try {
					ip = InetAddress.getByName(peer);
				} catch (UnknownHostException e) {
					throw new UnknownHostException("Host or IP address '" + peer + 
							"' could not be determined");
				}
				
				tokenMap.put(ip, rs.getString("token"));
			}
		} catch (SQLException e) {
			log.error("Error in loading tokens", e);
		} catch (UnknownHostException e) {
			log.error(e.getMessage(), e);
		}
		
		return tokenMap;
	}
	
}
