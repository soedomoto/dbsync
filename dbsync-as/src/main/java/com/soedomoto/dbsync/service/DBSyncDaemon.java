package com.soedomoto.dbsync.service;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.soedomoto.dbsync.config.DatabaseDescriptor;
import com.soedomoto.dbsync.db.SystemDB;
import com.soedomoto.dbsync.thrift.MySQL;
import com.soedomoto.dbsync.thrift.ThriftServer;

public class DBSyncDaemon {
	private static final Logger log = LoggerFactory.getLogger(DBSyncDaemon.class);
	private static final DBSyncDaemon instance = new DBSyncDaemon();
	
	public void activate() {
		try {
			try {
				DatabaseDescriptor.forceStaticInitialization();
			} catch (ExceptionInInitializerError e) {
				throw e.getCause();
			}
			
			setup();
			
			String pidFile = System.getProperty("pidfile");
            if (pidFile != null) {
                new File(pidFile).deleteOnExit();
            }

            if (System.getProperty("foreground") == null) {
                System.out.close();
                System.err.close();
            }
			
			//start()
		} catch (Throwable e) {
			log.error("Exception encountered during startup", e);
		}
	}
	
	private void setup() {
		logSystemInfo();
		
		SystemDB.instance.getTokens();
		SystemDB.instance.finishStartup();
		
		// Start transport
		MySQL db = null;
		try {
			db = new MySQL("jdbc:mysql://localhost/itbacademic", "root", "root");
		} catch (ClassNotFoundException e) {
			log.error("No MySQL driver found", e);
		} catch (SQLException e) {
			log.error("Error in executing query", e);
		}
		
		InetAddress address = DatabaseDescriptor.getListenAddress();
		Integer port = DatabaseDescriptor.getListenPort();
		Server thrift = new ThriftServer(address, port, db);
		thrift.start();
	}
	
	private void logSystemInfo() {
		log.info("-----------------------------------------------------------");
		log.info("System Info : ");
		log.info("-----------------------------------------------------------");
		try {
            log.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
        	log.info("Could not resolve local host");
        }

		log.info("JVM vendor/version: {}/{}", System.getProperty("java.vm.name"), System.getProperty("java.version"));
		log.info("Heap size: {}/{}", Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory());

        for(MemoryPoolMXBean pool: ManagementFactory.getMemoryPoolMXBeans())
        	log.info("{} {}: {}", pool.getName(), pool.getType(), pool.getPeakUsage());

        log.info("Classpath: {}", System.getProperty("java.class.path"));
        log.info("-----------------------------------------------------------");
	}

	public static void main(String[] args) {
		DBSyncDaemon.instance.activate();
	}
	
	public static interface Server {
		public void start();
		public void stop();
		public boolean isRunning();
	}
	
	public static interface Client {
		public void setCurrentAddress(SocketAddress addr);
	}
	
}
