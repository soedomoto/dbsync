package com.soedomoto.dbsync.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.soedomoto.dbsync.locator.DefaultEndpoint;
import com.soedomoto.dbsync.locator.Endpoint;

public class DatabaseDescriptor {	
	public static final String DEFAULT_LISTEN_ADDRESS = "127.0.0.1";
	public static final Integer DEFAULT_LISTEN_PORT = 9999;
	
	// Name : listen.address
	private static InetAddress listenAddress;
	// Name : broadcast.address
	private static InetAddress broadcastAddress;
	// Name : cluster.name
	private static String clusterName;
	// Name : endpoint.class
	private static Endpoint endpoint;
	// Name : listen.port
	private static Integer listenPort;

	public static void forceStaticInitialization() {}
	static {
		try {
			if(! Config.isClientMode) {
				Config.instance = new Config(Config.getConfigURL());
			}
			
			applyConfig();
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	public static void applyConfig() throws ConfigurationException {
		if((clusterName = Config.instance.get("cluster.name")) == null) {
			throw new ConfigurationException("Cluster name 'cluster.name' is not set");
		}
		
		if(Config.instance.get("listen.address") != null) {
			try {
				listenAddress = InetAddress.getByName(Config.instance.get("listen.address"));
			} catch (UnknownHostException e) {
				throw new ConfigurationException("Unknown listen.address '" + Config.instance.get("listen.address") + "'");
			}
			
			if(listenAddress.isAnyLocalAddress())
				throw new ConfigurationException("listen_address cannot be a wildcard address (" + 
						Config.instance.get("listen.address") + ")!");
		} else {
			try {
				listenAddress = InetAddress.getByName(DEFAULT_LISTEN_ADDRESS);
			} catch (UnknownHostException e) {
				throw new ConfigurationException("listen.address 'localhost' could not be determined");
			}
		}
		
		if(Config.instance.get("listen.port") != null) {
			listenPort = Integer.valueOf(Config.instance.get("listen.port"));
		} else {
			listenPort = DEFAULT_LISTEN_PORT;
		}
		
		if(Config.instance.get("broadcast.address") != null) {
			try {
				broadcastAddress = InetAddress.getByName(Config.instance.get("broadcast.address"));
			} catch (UnknownHostException e) {
				throw new ConfigurationException("Unknown listen.address '" + Config.instance.get("broadcast.address") + "'");
			}
			
			if(broadcastAddress.isAnyLocalAddress())
				throw new ConfigurationException("listen_address cannot be a wildcard address (" + 
						Config.instance.get("broadcast.address") + ")!");
		} else {
			broadcastAddress = listenAddress;
		}
		
		if(Config.instance.get("endpoint.class") != null) {
			endpoint = createEndpoint(Config.instance.get("endpoint.class"));
		} else {
			endpoint = new DefaultEndpoint();
		}
	}
	
	private static Endpoint createEndpoint(String classname) throws ConfigurationException {
		return construct(classname);
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T construct(String classname) throws ConfigurationException {
		try {
			Class<T> cls = (Class<T>) Class.forName(classname);
			return (T) cls;
		} catch (ClassNotFoundException e) {
			throw new ConfigurationException("Class '" + Config.instance.get("endpoint.class") + "' is not found in classpath");
		}
	}
	
	public static InetAddress getListenAddress() {
		return listenAddress;
	}

	public static void setListenAddress(InetAddress listenAddress) {
		DatabaseDescriptor.listenAddress = listenAddress;
	}

	public static InetAddress getBroadcastAddress() {
		return broadcastAddress;
	}

	public static void setBroadcastAddress(InetAddress broadcastAddress) {
		DatabaseDescriptor.broadcastAddress = broadcastAddress;
	}

	public static String getClusterName() {
		return clusterName;
	}

	public static void setClusterName(String clusterName) {
		DatabaseDescriptor.clusterName = clusterName;
	}

	public static Endpoint getEndpoint() {
		return endpoint;
	}

	public static void setEndpoint(Endpoint endpoint) {
		DatabaseDescriptor.endpoint = endpoint;
	}

	public static Integer getListenPort() {
		return listenPort;
	}

	public static void setListenPort(Integer listenPort) {
		DatabaseDescriptor.listenPort = listenPort;
	}
	
}
