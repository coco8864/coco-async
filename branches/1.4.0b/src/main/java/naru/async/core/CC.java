package naru.async.core;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.ChannelStastics;
import naru.async.ChannelHandler.IpBlockType;
import naru.async.core.ChannelContext.IO;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public class CC extends PoolBase{
	private static Logger logger=Logger.getLogger(CC.class);
	private static HashSet<CC> channelContexts=new HashSet<CC>();
	private static ChannelStastics totalChannelStastics=new  ChannelStastics();
	
	public static ChannelStastics getTotalChannelStastics(){
		return totalChannelStastics;
	}
	
	public static void dumpChannelContexts(){
		dumpChannelContexts(logger);
	}
	public static void dumpChannelContexts(Logger logger){
		Object[] cs=channelContexts.toArray();
		logger.info("channelContext count:"+cs.length);
		for(int i=0;i<cs.length;i++){
			ChannelContext c=(ChannelContext)cs[i];
			c.dump(logger);
		}
	}
	
	public void activate() {
		channelContexts.add(this);
	}

	public void inactivate() {
		synchronized(totalChannelStastics){
			totalChannelStastics.sum(stastics);
		}
		channelContexts.remove(this);
	}
	
	private ReadChannel readChannel;
	private WriteChannel writeChannel;
	private ChannelStastics stastics=new ChannelStastics();
	
	private SelectableChannel channel;
	private Socket socket;
	private ServerSocket serverSocket;
	private String remoteIp=null;
	private int remotePort=-1;
	private String localIp=null;
	private int localPort=-1;
	
	private static CC dummyContext=new CC();
	public static CC getDummyContext(){
		return dummyContext;
	}
	
	public static CC childContext(CC orgContext){
		CC context=(CC)PoolManager.getInstance(CC.class);
		context.remoteIp=orgContext.remoteIp;
		context.remotePort=orgContext.remotePort;
		context.localIp=orgContext.localIp;
		context.localPort=orgContext.localPort;
		return context;
	}
	
	public static CC create(ChannelHandler handler,SelectableChannel channel){
		CC context=(CC)PoolManager.getInstance(CC.class);
		context.setHandler(handler);
		context.channel=channel;
		context.selector=IOManager.getSelectorContext(context);
		context.readBuffer.setup();
		context.writeBuffer.setup();
		logger.debug("ChannelContext#create cid:"+context.getPoolId()+":ioStatus:"+context.ioStatus +":handler:"+handler.getPoolId()+":"+channel);
		if(channel instanceof SocketChannel){
			context.setIoStatus(IO.SELECT);
			context.socket=((SocketChannel)channel).socket();
			InetAddress inetAddress=context.socket.getInetAddress();
			context.remotePort=context.socket.getPort();
			if(inetAddress!=null){
				context.remoteIp=inetAddress.getHostAddress();
			}
			context.localPort=context.socket.getLocalPort();
			inetAddress=context.socket.getLocalAddress();
			if(inetAddress!=null){
				context.localIp=inetAddress.getHostAddress();
			}
			context.serverSocket=null;
		}else if(channel instanceof ServerSocketChannel){
			context.serverSocket=((ServerSocketChannel)channel).socket();
			context.localIp=context.remoteIp=null;
			context.localPort=context.remotePort=-1;
			context.socket=null;
		}
		return context;
	}
	
	public boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass){
	}
	
	public boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,IpBlockType ipBlockType,Pattern blackList,Pattern whiteList){
	}

	public boolean asyncConnect(Object userContext,String remoteHost,int remotePort,long timeout){
	}
	
	public boolean asyncConnect(Object userContext,InetSocketAddress address,long timeout){
	}
	
	public boolean asyncRead(Object userContext){
	}
	
	public boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
	}
	
	public boolean asyncClose(Object userContext){
	}

}
