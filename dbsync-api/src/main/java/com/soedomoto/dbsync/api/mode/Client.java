package com.soedomoto.dbsync.api.mode;

import java.net.SocketAddress;
import java.sql.SQLException;
import java.util.Properties;

public interface Client {
	public void connect(String url, String username, String password, Properties prop) throws SQLException;
	public void setCurrentAddress(SocketAddress addr);
}
