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
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.ChannelStastics;
import naru.async.ChannelHandler.IpBlockType;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public class ChannelContext extends PoolBase{
	private static Logger logger=Logger.getLogger(ChannelContext.class);
	private static HashSet<ChannelContext> ChannelContexts=new HashSet<ChannelContext>();
	private static ChannelStastics totalChannelStastics=new  ChannelStastics();
	
	public static ChannelStastics getTotalChannelStastics(){
		return totalChannelStastics;
	}
	
	public static void dumpChannelContexts(){
		dumpChannelContexts(logger);
	}
	public static void dumpChannelContexts(Logger logger){
		Object[] cs=ChannelContexts.toArray();
		logger.info("ChannelContext count:"+cs.length);
		for(int i=0;i<cs.length;i++){
			ChannelContext c=(ChannelContext)cs[i];
			c.dump(logger);
		}
	}
	
	private static ThreadLocal<ChannelContext> currentContext=new ThreadLocal<ChannelContext>();
	private LinkedList<Order> callbackOrders=new LinkedList<Order>();
	private boolean inCallback=false;
	/* callback関連 */
	public void queueCallback(Order order){
		synchronized(callbackOrders){
			callbackOrders.add(order);
			logger.debug("queueCallback cid:"+getPoolId() +":size:"+callbackOrders.size()+":"+order.getOrderType()+ ":" +handler);
			if(inCallback==false){
				inCallback=true;
				DispatchManager.enqueue(this);
			}
		}
	}
	
	/**
	 * dispatch workerから呼び出される、多重では走行しない
	 */
	public void callback(){
		Order order=null;
		boolean isFinishCallback=false;
		ChannelHandler finishHandler=null;
		currentContext.set(this);
		try{
			while(true){
				synchronized(callbackOrders){
					logger.debug("callback cid:"+getPoolId() +":size:"+callbackOrders.size());
					if(order!=null){
						callbackOrders.remove(order);
					}
					if(callbackOrders.size()==0){
						inCallback=false;
						break;
					}
					order=(Order)callbackOrders.get(0);
				}
				if(order.isFinish()){
					isFinishCallback=true;
					finishHandler=order.getHandler();
				}
				try{
					order.callback(stastics);
				}catch(Throwable t){
					logger.warn("callback return Throwable",t);
					//回線が切断されたとして処理
					//doneClosed(true);
				}
			}
		}finally{
			currentContext.set(null);
			if(isFinishCallback==false){
				return;
			}
			logger.debug("callback isFinishCallback=true.cid:"+getPoolId());
			if(finishHandler!=handler){
				logger.warn("finish callback finishHandler:"+finishHandler);
				logger.warn("finish callback handler:"+handler);
			}
			readBuffer.cleanup();
			writeBuffer.cleanup();
			finishHandler.unref();
			setHandler(null);
			unref();
		}
	}
	
	public void dump(){
		dump(logger);
	}
	
	public void dump(Logger logger){
		StringBuffer sb=new StringBuffer("[");
		sb.append("cid:");
		sb.append(getPoolId());
		sb.append(":ioStatus:");
		sb.append(ioStatus);
		sb.append(":handler:");
		sb.append(handler);
		if(selector!=null){
			sb.append(":selectorId:");
			sb.append(selector.getId());
		}
		sb.append(":socket:");
		sb.append(socket);
		sb.append(":");
		sb.append(super.toString());
		
		logger.debug(sb.toString());
		if(handler!=null){
			handler.dump(logger);
		}
		//writeBuffer.dump(logger);
		//readBuffer.dump(logger);
		orders.dump(logger);
		logger.debug("]");
	}
	
	public void activate() {
		ChannelContexts.add(this);
	}

	public void inactivate() {
		synchronized(totalChannelStastics){
			totalChannelStastics.sum(stastics);
		}
		ChannelContexts.remove(this);
	}
	
	private ChannelStastics stastics=new ChannelStastics();
	private ReadChannel readChannel=new ReadChannel();
	private WriteChannel writeChannel=new WriteChannel();
	private ContextOrders orders=new ContextOrders(this);
	
	
	private SelectableChannel channel;
	private Socket socket;
	private ServerSocket serverSocket;
	private String remoteIp=null;
	private int remotePort=-1;
	private String localIp=null;
	private int localPort=-1;
	
	private Map attribute=new HashMap();//handlerに付随する属性
	private SelectorContext selector;
	private SelectionKey selectionKey;//IO_SELECTの場合有効
	private ChannelHandler handler;
	private boolean isFinished;
	
	private static ChannelContext dummyContext=new ChannelContext();
	public static ChannelContext getDummyContext(){
		return dummyContext;
	}
	
	public static ChannelContext childContext(ChannelContext orgContext){
		ChannelContext context=(ChannelContext)PoolManager.getInstance(ChannelContext.class);
		context.remoteIp=orgContext.remoteIp;
		context.remotePort=orgContext.remotePort;
		context.localIp=orgContext.localIp;
		context.localPort=orgContext.localPort;
		return context;
	}
	
	public static ChannelContext create(ChannelHandler handler,SelectableChannel channel){
		ChannelContext context=(ChannelContext)PoolManager.getInstance(ChannelContext.class);
		context.setHandler(handler);
		context.channel=channel;
		context.selector=IOManager.getSelectorContext(context);
		context.readBuffer.setup();
		context.writeBuffer.setup();
		logger.debug("ChannelContext#create cid:"+context.getPoolId()+":ioStatus:"+context.ioStatus +":handler:"+handler.getPoolId()+":"+channel);
		if(channel instanceof SocketChannel){
			//context.setIoStatus(IO.SELECT);
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
	
	public void setHandler(ChannelHandler handler){
		this.handler=handler;
	}
	
	public Object getAttribute(String name){
		return attribute.get(name);
	}
	
	public void setAttribute(String name, Object value) {
		synchronized(attribute){
			attribute.put(name, value);
		}
	}
	
	public boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,IpBlockType ipBlockType,Pattern blackList,Pattern whiteList){
		return false;
	}
	
	public boolean asyncConnect(Object userContext,InetSocketAddress address,long timeout){
		return false;
	}
	
	public boolean asyncRead(Object userContext){
		return false;
	}
	
	public boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
		return false;
	}
	
	public boolean asynChannelContextlose(Object userContext){
		return false;
	}

}
