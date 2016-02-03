package com.soedomoto.dbsync.thrift.old;

import java.net.SocketAddress;
import java.sql.SQLException;

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
import com.soedomoto.dbsync.thrift.MySQL;

public class Server {
	private static Logger log = LoggerFactory.getLogger(Server.class);
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		final MySQL db = new MySQL("jdbc:mysql://localhost/itbacademic", "root", "root");
		
		ThriftServiceProcessor processor = new ThriftServiceProcessor(
            new ThriftCodecManager(),
            ImmutableList.<ThriftEventHandler>of(),
            db
        ) {
			@Override
			public ListenableFuture<Boolean> process(TProtocol in, TProtocol out, RequestContext requestContext)
					throws TException {
				SocketAddress client = requestContext.getConnectionContext().getRemoteAddress();
				//db.setCurrentClient(client);
				
				return super.process(in, out, requestContext);
			}
		};
		
		ThriftServerDef serverDef = ThriftServerDef.newBuilder()
            .listen(9090)
            .withProcessor(processor)
            .protocol(new TBinaryProtocol.Factory())
            .build();
		
		NettyServerConfigBuilder nettyConfigBuilder = new NettyServerConfigBuilder();
        nettyConfigBuilder.getSocketChannelConfig().setTcpNoDelay(true);
        nettyConfigBuilder.getSocketChannelConfig().setConnectTimeoutMillis(5000);
        NettyServerConfig serverConfig = nettyConfigBuilder.build();
        
        NettyServerTransport server = new NettyServerTransport(serverDef, serverConfig, new DefaultChannelGroup() {
        	@Override
        	public boolean add(Channel channel) {
        		log.info("New client " + channel.getRemoteAddress().toString() + " is connected");
        		return super.add(channel);
        	}
        });
        server.start();
	}
	
}
