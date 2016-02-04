package com.soedomoto.dbsync.thrift;

import java.net.InetAddress;
import java.net.SocketAddress;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.nifty.core.NettyServerConfig;
import com.facebook.nifty.core.NettyServerConfigBuilder;
import com.facebook.nifty.core.NettyServerTransport;
import com.facebook.nifty.core.RequestContext;
import com.facebook.nifty.core.ThriftServerDef;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.soedomoto.dbsync.api.mode.Client;
import com.soedomoto.dbsync.api.mode.Server;
import com.soedomoto.dbsync.service.DBSyncDaemon;

public class ThriftServer implements Server {
	private static final Logger log = LoggerFactory.getLogger(ThriftServer.class);

	private InetAddress address;
	private int port;
	private Client[] services;
	
	private ThriftServerThread server;

	public ThriftServer(InetAddress address, int port, Client... services) {
		this.address = address;
		this.port = port;
		this.services = services;
	}

	@Override
	public void start() {
		if(server == null) {
			server = new ThriftServerThread(address, port, services);
			server.start();
		}
	}

	@Override
	public void stop() {
		if(server != null) {
			server.stopServer();
			
			try {
				server.join();
			} catch (InterruptedException e) {
				log.error("Interrupted while waiting thrift server to stop", e);
			}
			
			server = null;
		}
	}

	@Override
	public boolean isRunning() {
		return server != null;
	}
	
	private static class ThriftServerThread extends Thread {
		private InetAddress address;
		private int port;
		private Client[] services;
		
		private NettyServerTransport server;

		public ThriftServerThread(InetAddress address, int port, Client... services) {
			this.address = address;
			this.port = port;
			this.services = services;
			
			log.info(String.format("Binding thrift service to %s:%s", address, port));
			
			ThriftServiceProcessor processor = new ThriftServiceProcessor(
	            new ThriftCodecManager(),
	            ImmutableList.<ThriftEventHandler>of(),
	            this.services
	        ) {
				@Override
				public ListenableFuture<Boolean> process(TProtocol in, TProtocol out, RequestContext requestContext)
						throws TException {
					SocketAddress addr = requestContext.getConnectionContext().getRemoteAddress();
					for(Client client : services) {
						client.setCurrentAddress(addr);
					}
					
					return super.process(in, out, requestContext);
				}
			};
			
			NettyServerConfigBuilder nettyConfigBuilder = new NettyServerConfigBuilder();
	        nettyConfigBuilder.getSocketChannelConfig().setTcpNoDelay(true);
	        nettyConfigBuilder.getSocketChannelConfig().setConnectTimeoutMillis(5000);
	        NettyServerConfig serverConfig = nettyConfigBuilder.build();
	        
	        ThriftServerDef serverDef = ThriftServerDef.newBuilder()
                .listen(this.port)
                .withProcessor(processor)
                .protocol(new TBinaryProtocol.Factory())
                .build();
	        
	        server = new NettyServerTransport(serverDef, serverConfig, new DefaultChannelGroup() {
	        	@Override
	        	public boolean add(Channel channel) {
	        		log.info("New client " + channel.getRemoteAddress().toString() + " is connected");
	        		return super.add(channel);
	        	}
	        });
		}
		
		@Override
		public void run() {
			log.info("Listening for thrift clients...");
			 server.start();
		}

		public void stopServer() {
			log.info("Stop listening to thrift clients");
			try {
				server.stop();
			} catch (InterruptedException e) {
				log.error("Interrupted while waiting thrift server to stop", e);
			}
		}
		
	}
	
}
