package com.soedomoto.dbsync.service;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.soedomoto.dbsync.api.mode.Client;
import com.soedomoto.dbsync.api.mode.Server;
import com.soedomoto.dbsync.config.Config;
import com.soedomoto.dbsync.config.DatabaseDescriptor;
import com.soedomoto.dbsync.db.SystemDB;
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
	
	@SuppressWarnings("unchecked")
	private void setup() {
		logSystemInfo();
		
		SystemDB.instance.getTokens();
		SystemDB.instance.finishStartup();
		
		// Create connection
		Client db = null;
		try {
			Class.forName(Config.instance.get("db.driver"));
			Class<? extends Client> clientImpl = (Class<? extends Client>) Class.forName(Config.instance.get("db.implementation"));
			db = clientImpl.newInstance();
			db.connect(
				Config.instance.get("db.url"), 
				Config.instance.get("db.properties.username"), 
				Config.instance.get("db.properties.password"), 
				new Properties()
			);
		} catch (ClassNotFoundException e) {
			log.error("No driver '" + Config.instance.get("db.driver") + "' found", e);
		} catch (SQLException e) {
			log.error("Error in executing query", e);
		} catch (InstantiationException | IllegalAccessException e) {
			log.error("Error in instantiation client implementation", e);
		}
		
		// Start thrift server
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
	
}
