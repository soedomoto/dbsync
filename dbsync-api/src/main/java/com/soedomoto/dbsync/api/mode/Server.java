package com.soedomoto.dbsync.api.mode;

public interface Server {
	public void start();
	public void stop();
	public boolean isRunning();
}
