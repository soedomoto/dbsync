package com.soedomoto.dbsync.locator;

import java.net.InetAddress;

public interface Endpoint {
	public String getRack(InetAddress endpoint);
	public String getDatacenter(InetAddress endpoint);
}
