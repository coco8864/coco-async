package naru.async.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
import naru.async.core.CC.IO;
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
	
	public void dump(){
		dump(logger);
	}
	
	public void dump(Logger logger){
		StringBuffer sb=new StringBuffer("[");
		sb.append("cid:");
		sb.append(getPoolId());
		//sb.append(":ioStatus:");
		//sb.append(ioStatus);
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
	private ReadChannel readChannel=new ReadChannel(this);
	private WriteChannel writeChannel=new WriteChannel(this);
	private ContextOrders orders=new ContextOrders(this);
	
	ContextOrders getContextOrders(){
		return orders;
	}
	
	ReadChannel getReadChannel(){
		return readChannel;
	}
	
	WriteChannel getWriteChannel(){
		return writeChannel;
	}
	
	private SelectableChannel channel;
	private Socket socket;
	private ServerSocket serverSocket;
	
	void closeSocket(){
		try {
			socket.close();
		} catch (IOException e) {
		}
	}
	
	void shutdownOutputSocket(){
		try {
			socket.shutdownOutput();
		} catch (IOException e) {
		}
	}
	
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
	
	public static ChannelContext serverChannelCreate(ChannelHandler handler,ServerSocketChannel channel){
		ChannelContext context=(ChannelContext)PoolManager.getInstance(ChannelContext.class);
		context.setHandler(handler);
		context.channel=channel;
		context.selector=IOManager.getSelectorContext(context);
		context.readChannel.setup(channel);
		context.writeChannel.setup(channel);
		context.serverSocket=((ServerSocketChannel)channel).socket();
		context.localIp=context.remoteIp=null;
		context.localPort=context.remotePort=-1;
		context.socket=null;
		return context;
	}
	
	public static ChannelContext socketChannelCreate(ChannelHandler handler,SocketChannel channel){
		ChannelContext context=(ChannelContext)PoolManager.getInstance(ChannelContext.class);
		context.setHandler(handler);
		context.channel=channel;
		context.selector=IOManager.getSelectorContext(context);
		context.readChannel.setup(channel);
		context.writeChannel.setup(channel);
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
		return context;
	}
	
	/**
	 * select処理を完了(以下２つのパターン)した場合
	 * 1)selectorから削除された nextWakeup
	 * 2)selectorに参加させた 次回のタイムアウト時刻
	 * selectorに参加させよとしたが、完了できなかった場合 -1
	 * @param selector
	 * @return　次回タイムアウト時刻
	 */
	public long select(SelectorContext selector,long nextWakeup){
		long now=System.currentTimeMillis();
		synchronized(ioLock){
			if(ioStatus!=IO.QUEUE_SELECT && ioStatus!=IO.SELECT ){
				if(selectionKey!=null){
					logger.debug("select#1 selectionKey.cancel().ioStatus:"+ioStatus);
					selectionKey.cancel();
					selectionKey=null;
				}
				return nextWakeup;
			}
			/* close要求が到着していたら、出力を閉じる */
			if(orders.isCloseable()){
				if(selectionKey!=null){
					logger.debug("select#2 selectionKey.cancel().");
					selectionKey.cancel();
					selectionKey=null;
				}
				closable(true);
				setIoStatus(IO.SELECT);//なぜこんなのが必要なのか？
				queueIO(IO.CLOSEABLE);//ここで結局IO.CLOSEABLEになる
				return nextWakeup;
			}
			/* timeout判定 */
			if(connectTimeoutTime<=now){
				orders.timeout(Order.TYPE_CONNECT);
				connectTimeoutTime=Long.MAX_VALUE;
			}else if (nextWakeup>connectTimeoutTime){
				nextWakeup=connectTimeoutTime;
			}
			
			if(readTimeoutTime<=now){
				orders.timeout(Order.TYPE_READ);
				readTimeoutTime=Long.MAX_VALUE;
			}else if (nextWakeup>readTimeoutTime){
				nextWakeup=readTimeoutTime;
			}
			
			if(writeTimeoutTime<=now){
				orders.timeout(Order.TYPE_WRITE);
				writeTimeoutTime=Long.MAX_VALUE;
			}else if (nextWakeup>writeTimeoutTime){
				nextWakeup=writeTimeoutTime;
			}
			
			int ops=operations();
			if(ioStatus==IO.SELECT){
				if(selectionKey.interestOps()!=ops){
					selectionKey.interestOps(ops);
				}
			}else if (ioStatus==IO.QUEUE_SELECT){
				try {
					if(channel.isRegistered()){
						logger.debug("IO_QUEUE_SELECT and isRegistered.cid:" + getPoolId() +":selectionKey:"+selectionKey);
//						logger.info("####cid:" +getPoolId() + " selector:"+selector + " isRegistered:" + channel.isRegistered());
						//新規にselectorに参加させようとしたが、すでに参加している?
						//selectionKey=channel.keyFor(selector);
						//前回IO可能になってから、IOが兆速攻で終わったので、前回IO、selectorがまだselectに入っていない
						//SELECTになることができない。１回休み。
						//想定した動作であり問題なし
						if(selectionKey!=null){
							logger.debug("cid:"+getPoolId() +":select#3 selectionKey.cancel().");
							selectionKey.cancel();
							selectionKey=null;
						}
						return -1;
					}
					//ここでcloseされていると以下の例外
					/*
2011-11-24 11:32:11,462 [selector-2] WARN  naru.async.core.ChannelContext - select aleady closed.
java.nio.channels.ClosedChannelException
        at java.nio.channels.spi.AbstractSelectableChannel.configureBlocking(AbstractSelectableChannel.java:252)
        at naru.async.core.ChannelContext.select(ChannelContext.java:736)
        at naru.async.core.SelectorContext.selectAll(SelectorContext.java:134)
					 */
					channel.configureBlocking(false);
					selectionKey=selector.register(channel, ops,this);
					//TODO 調査のためにinfo
					//logger.debug("cid:"+getPoolId() +":ops:"+ops);
					setIoStatus(IO.SELECT);
				} catch (ClosedChannelException e) {
					logger.warn("select aleady closed.",e);
//					doneClosed(true);
					failure(e);
				} catch (IOException e) {
					logger.error("select io error.",e);
					failure(e);
				}
			}
			return nextWakeup;
		}
	}
	
	boolean isConnected(){
		if(socket==null){
			return false;
		}
		return socket.isConnected();
	}
	
	public synchronized boolean foward(ChannelHandler handler){
		setHandler(handler);
		return true;
	}
	
	public void setHandler(ChannelHandler handler){
		if(this.handler!=null){
			this.handler.unref();
		}
		if(handler!=null){
			handler.ref();
		}
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
	
	/* accept関連 */
	private Class acceptClass;
	private Object acceptUserContext;
	private IpBlockType ipBlockType;
	private Pattern blackList;
	private Pattern whiteList;
	
	public synchronized boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,IpBlockType ipBlockType,Pattern blackList,Pattern whiteList){
		if(orders.acceptOrder(userContext)==false){
			return false;
		}
		this.acceptClass=acceptClass;
		this.acceptUserContext=acceptUserContext;
		this.ipBlockType=ipBlockType;
		this.blackList=blackList;
		this.whiteList=whiteList;
		stastics.asyncAccept();
		return true;
	}

	public synchronized boolean asyncConnect(Object userContext,InetSocketAddress address,long timeout){
		if(orders.connectOrder(userContext)==false){
			return false;
		}
		stastics.asyncConnect();
		return true;
	}
	
	public synchronized boolean asyncRead(Object userContext){
		if(orders.readOrder(userContext)==false){
			return false;
		}
		if(orders.isReadOrder()&&readTimeout>0){
			readTimeoutTime=System.currentTimeMillis()+readTimeout;
		}
		stastics.asyncRead();
		return true;
	}
	
	public synchronized boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
		long length=BuffersUtil.remaining(buffers);
		long asyncWriteStartOffset=stastics.getAsyncWriteLength();		
		if(orders.writeOrder(userContext,buffers,asyncWriteStartOffset,length)==false){
			return false;
		}
		if(orders.isReadOrder()&&writeTimeout>0&&writeTimeoutTime<0){
			writeTimeoutTime=System.currentTimeMillis()+writeTimeout;
		}
		stastics.addAsyncWriteLength(length);
		stastics.asyncWrite();
		return true;
	}
	
	public synchronized boolean asyncClose(Object userContext){
		if( orders.closeOrder(userContext)==false ){
			return false;
		}
		stastics.asyncClose();
		return true;
	}

	public long getTotalReadLength() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	private long readTimeoutTime;
	private long writeTimeoutTime;

	private long readTimeout;
	private long writeTimeout;
	
	public long getReadTimeout() {
		return readTimeout;
	}
	
	public void setReadTimeout(long readTimeout) {
		this.readTimeout=readTimeout;
	}

	public long getWriteTimeout() {
		return writeTimeout;
	}

	public void setWriteTimeout(long writeTimeout) {
		this.writeTimeout=writeTimeout;
	}
	
	public String getRemoteIp(){
		return	remoteIp;
	}
	
	public int getRemotePort(){
		return	remotePort;
	}
	
	public String getLocalIp(){
		return	localIp;
	}
	
	public int getLocalPort(){
		return	localPort;
	}

	ChannelHandler getHandler() {
		return handler;
	}

	SelectorContext getSelector() {
		return selector;
	}
	
}
