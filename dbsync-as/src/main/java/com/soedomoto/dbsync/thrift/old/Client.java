package com.soedomoto.dbsync.thrift.old;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientConfig;
import com.facebook.swift.service.ThriftClientManager;
import com.soedomoto.dbsync.thrift.DBMS;

public class Client {
	private static Logger log = LoggerFactory.getLogger(Client.class);
	
	public static void main(String[] args) throws InterruptedException, ExecutionException, SQLException {
		InetSocketAddress address = new InetSocketAddress("localhost", 9090);
		ThriftClientManager manager = new ThriftClientManager();
		ThriftClientConfig config = new ThriftClientConfig();
		
		ThriftClient<DBMS> thriftClient = new ThriftClient<>(manager, DBMS.class, config, "Client");
		DBMS dbms = thriftClient.open(new FramedClientConnector(address,
                TDuplexProtocolFactory.fromSingleFactory(new TBinaryProtocol.Factory()))).get();
		
		for(String table : dbms.getTables()) {
			log.info(String.format("Content : %s ---------------------", table));
			Integer offset = dbms.getCurrentOffset(table);
			for(List<byte[]> row : dbms.getRows(table, offset, 10)) {
				String[] str = new String[row.size()];
				int i = 0;
				for(byte[] col : row) {
					str[i] = String.valueOf(col);
					i++;
				}
				
				log.info(Arrays.toString(str));
			}
		}
	}

}
