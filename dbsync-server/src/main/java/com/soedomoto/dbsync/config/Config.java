package com.soedomoto.dbsync.config;

import java.io.File;
import java.net.URL;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	private static final Logger log = LoggerFactory.getLogger(Config.class);
	private final static String DEFAULT_CONFIGURATION = "dbsync.properties";
	
	private Configuration config;
	
	// Configuration properties start here
	public static boolean isClientMode = false;
	
	public static Config instance = new Config();

	public Config() {}
	public Config(URL url) {
		try {
			log.info("Loading settings from {}", url);
			
			config = new PropertiesConfiguration(url);
		} catch (org.apache.commons.configuration.ConfigurationException e) {
			log.error("Invalid configurations " + url, e);
		}
	}
	
	public static URL getConfigURL() throws ConfigurationException {
		String configUrl = System.getProperty("config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;
        
        URL url;
        try {
        	url = new URL(configUrl);
        	url.openStream().close();
        } catch (Exception e) {
        	url = Config.class.getClassLoader().getResource(configUrl);
        	if (url == null) {
        		String required = "file:" + File.separator + File.separator;
                if (!configUrl.startsWith(required))
                    throw new ConfigurationException("Expecting URI in variable: [dbsync.config]. " + 
                    		"Please prefix the file with " + required + File.separator +
                            " for local files or " + required + "<server>" + File.separator + 
                            " for remote files.");
                throw new ConfigurationException("Cannot locate " + configUrl + 
                		".  If this is a local file, please confirm you've provided " + required + 
                		File.separator + " as a URI prefix.");
        	}
        }
        
        return url;
	}
	
	public static boolean isClientMode() {
		return Config.isClientMode;
	}

	public static void setClientMode(boolean isClientMode) {
		Config.isClientMode = isClientMode;
	}
	
	public void set(String key, String value) {
		config.setProperty(key, value);
	}
	
	public String get(String key) {
		return config.getString(key);
	}

}
