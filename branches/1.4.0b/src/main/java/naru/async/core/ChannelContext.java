package naru.async.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.ChannelStastics;
import naru.async.core.Order.OrderType;
import naru.async.core.SelectOperator.State;
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
	private SelectOperator selectOperator=new SelectOperator(this);
	private WriteOperator writeOperator=new WriteOperator(this);
	private OrderOperator orderOperator=new OrderOperator(this);
	
	ChannelStastics getChannelStastics(){
		return stastics;
	}
	
	OrderOperator getOrderOperator(){
		return orderOperator;
	}
	
	SelectOperator getSelectOperator(){
		return selectOperator;
	}
	
	WriteOperator getWriteOperator(){
		return writeOperator;
	}
	
	private SelectableChannel channel;
	private Socket socket;
	private ServerSocket serverSocket;
	
	void closeSocket(){
		try {
			if(socket!=null){
				socket.close();
			}else if(serverSocket!=null){
				serverSocket.close();
			}
		} catch (IOException e) {
		}
	}
	
	void shutdownOutputSocket(){
		try {
			if(socket!=null){
				socket.shutdownOutput();
			}else if(serverSocket!=null){
				serverSocket.close();
			}
		} catch (IOException e) {
		}
	}
	
	private String remoteIp=null;
	private int remotePort=-1;
	private String localIp=null;
	private int localPort=-1;
	
	private Map attribute=new HashMap();//handlerに付随する属性
	private SelectorHandler selector;
	private SelectionKey selectionKey;//IO_SELECTの場合有効
	private long nextSelectWakeUp;
	long getNextSelectWakeUp() {
		return nextSelectWakeUp;
	}

	private ChannelHandler handler;
	
	private static ChannelContext dummyContext=new ChannelContext();
	public static ChannelContext getDummyContext(){
		return dummyContext;
	}
	
	private static ChannelContext create(ChannelHandler handler,SelectableChannel channel){
		ChannelContext context=(ChannelContext)PoolManager.getInstance(ChannelContext.class);
		context.setHandler(handler);
		context.channel=channel;
		context.selector=IOManager.getSelectorContext(context);
		context.selectOperator.setup(channel);
		context.writeOperator.setup(channel);
		context.orderOperator.setup();
		return context;
	}
	
	public static ChannelContext serverChannelCreate(ChannelHandler handler,InetSocketAddress address,int backlog){
		/* contextを作る */
		ServerSocketChannel serverSocketChannel;
		try {
			serverSocketChannel = ServerSocketChannel.open();
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.socket().bind(address,backlog);
		} catch (IOException e) {
			logger.error("failt to asyncAccept.",e);
			return null;
		}
		
		ChannelContext context=create(handler,serverSocketChannel);
		context.serverSocket=serverSocketChannel.socket();
		context.localIp=context.remoteIp=null;
		context.localPort=context.remotePort=-1;
		context.socket=null;
		return context;
	}
	static ChannelContext socketChannelCreate(ChannelHandler handler,SocketChannel channel){
		ChannelContext context=create(handler,channel);
		context.socket=channel.socket();
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
	
	public static ChannelContext socketChannelCreate(ChannelHandler handler,InetSocketAddress address){
		SocketChannel channel;
		try {
			channel=SocketChannel.open();
			channel.configureBlocking(false);
			if(channel.connect(address)){
				//connectが完了しちゃった
			}
		} catch (IOException e) {
			logger.error("fail to connect",e);
			return null;
		}
		return socketChannelCreate(handler, channel);
	}
	
	private int acceptingSelect(long now){
		if(orderOperator.isCloseOrder()){
			selectOperator.queueIo(State.closing);
			return 0;
		}
		return SelectionKey.OP_ACCEPT;
	}
	
	private int connectingSelect(long now){
		if(orderOperator.isCloseOrder()){
			selectOperator.queueIo(State.closing);
			return 0;
		}
		long time=orderOperator.checkConnectTimeout(now);
		if(time==OrderOperator.CHECK_TIMEOUT){
			orderOperator.timeout(OrderType.connect);
			selectOperator.queueIo(State.closing);
			return 0;
		}else if(time>0){
			updateNextSelectWakeUp(time);
		}
		return SelectionKey.OP_CONNECT;
	}
	
	private void updateNextSelectWakeUp(long timeoutTime){
		if(nextSelectWakeUp<=timeoutTime){
			return;
		}
		nextSelectWakeUp=timeoutTime;
	}
	
	private int readingSelect(long now){
		long time=orderOperator.checkReadTimeout(now);
		if(time==OrderOperator.CHECK_TIMEOUT){
			orderOperator.timeout(OrderType.read);
			if(writeOperator.isClose()){
				selectOperator.queueIo(State.closing);
				return 0;
			}
			//readはtimeoutしても処理続行
		}else if(time>0){
			updateNextSelectWakeUp(time);
		}
		int ops=SelectionKey.OP_READ;
		if(!writeOperator.isBlock()){
			return ops;
		}
		time=orderOperator.checkWriteTimeout(now);
		if(time==OrderOperator.CHECK_TIMEOUT){
			//writeがtimeoutしたらfailureとして処理
			orderOperator.timeout(OrderType.write);
			selectOperator.queueIo(State.closing);
			return 0;
		}else if(time>0){
			ops|=SelectionKey.OP_WRITE;
			updateNextSelectWakeUp(time);
		}
		return ops;
	}
	
	void cancelSelect(){
		if(selectionKey==null){
			return;
		}
		selectionKey.cancel();
		selectionKey=null;
	}
	
	private void failureEnd(){
		orderOperator.failure(null);
		selectOperator.queueIo(State.closing);
	}
	
	synchronized boolean select(){
		long now=System.currentTimeMillis();
		nextSelectWakeUp=Long.MAX_VALUE;
		int ops=0;
		switch(selectOperator.getState()){
		case accepting:
			ops=acceptingSelect(now);
			break;
		case selectConnecting:
			ops=connectingSelect(now);
		case connecting:
			break;
		case selectReading:
			ops=readingSelect(now);
		case reading:
			break;
		default:
			//error
			//logger.error("select state error.cid:"+getPoolId()+":"+selectOperator.getState());
			//failureEnd();
			return false;
		}
		if(ops==0){//selectを続ける必要なし
			return false;
		}
		try {
			if(selectionKey==null){
				channel.configureBlocking(false);
				try {
					selectionKey=selector.register(channel, ops,this);
				} catch (CancelledKeyException e) {
					logger.error("select state error.cid:"+getPoolId()+":"+selectOperator.getState());
					failureEnd();
					return false;
				}
			}else if(selectionKey.interestOps()!=ops){
				selectionKey.interestOps(ops);
			}
		} catch (ClosedChannelException e) {
			failureEnd();
			return false;
		} catch (IOException e) {
			failureEnd();
			return false;
		}
		return true;
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
	private boolean isBlockOutOfList;
	private Pattern blackList;
	private Pattern whiteList;
	
	public synchronized boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,boolean isBlockOutOfList,Pattern blackList,Pattern whiteList){
		if(orderOperator.acceptOrder(userContext)==false){
			return false;
		}
		this.acceptClass=acceptClass;
		this.acceptUserContext=userContext;
		this.isBlockOutOfList=isBlockOutOfList;
		this.blackList=blackList;
		this.whiteList=whiteList;
		stastics.asyncAccept();
		return true;
	}

	public synchronized boolean asyncConnect(Object userContext,InetSocketAddress address,long timeout){
		if(orderOperator.connectOrder(userContext,timeout)==false){
			return false;
		}
		stastics.asyncConnect();
		return true;
	}
	
	public synchronized boolean asyncRead(Object userContext){
		long timeoutTime=Long.MAX_VALUE;
		if(readTimeout>0){
			timeoutTime=System.currentTimeMillis()+readTimeout;
		}
		if(orderOperator.readOrder(userContext,timeoutTime)==false){
			return false;
		}
		stastics.asyncRead();
		return true;
	}
	
	public synchronized boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
		long length=BuffersUtil.remaining(buffers);
		long asyncWriteStartOffset=stastics.getAsyncWriteLength();
		long timeoutTime=Long.MAX_VALUE;
		if(writeTimeout>0){
			timeoutTime=System.currentTimeMillis()+writeTimeout;
		}
		if(orderOperator.writeOrder(userContext,buffers,asyncWriteStartOffset,length,timeoutTime)==false){
			return false;
		}
		stastics.addAsyncWriteLength(length);
		stastics.asyncWrite();
		return true;
	}
	
	public synchronized boolean asyncClose(Object userContext){
		if( orderOperator.closeOrder(userContext)==false ){
			return false;
		}
		stastics.asyncClose();
		return true;
	}

	public long getTotalReadLength() {
		return selectOperator.getTotalReadLength();
	}
	
	public long getTotalWriteLength() {
		return writeOperator.getTotalWriteLength();
	}
	
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

	SelectorHandler getSelector() {
		return selector;
	}

	public Class getAcceptClass() {
		return acceptClass;
	}

	public Object getAcceptUserContext() {
		return acceptUserContext;
	}
	
	private boolean matchPattern(Pattern pattern,String ip){
		if(pattern==null){
			return false;
		}
		Matcher matcher;
		synchronized(pattern){
			matcher=pattern.matcher(ip);
		}
		return matcher.matches();
	}
	
	boolean acceptable(Socket socket){
		String clietnIp=socket.getInetAddress().getHostAddress();
		if(isBlockOutOfList){//whiteListに載ってない、けどbackListに載ってなければ許可
			if( !matchPattern(whiteList,clietnIp) ){
				if( matchPattern(blackList,clietnIp) ){
					return false;
				}
			}
		}else{//blackListに載ってたら無条件で許否、whiteListに載ってないのも許否
			if( matchPattern(blackList,clietnIp) ){
				return false;
			}
			if( !matchPattern(whiteList,clietnIp) ){
				return false;
			}
		}
		Order order=Order.create(handler, OrderType.select, acceptUserContext);
		orderOperator.queueCallback(order);
		return true;
	}
	
	public void accepted(Object userContext){
		Order order=Order.create(handler, OrderType.accept, null);
		order.setUserCountexts(new Object[]{this,userContext});
		orderOperator.queueCallback(order);
	}
	
	/*--- for SPDY ---*/
	public static ChannelContext childContext(ChannelContext orgContext){
		ChannelContext context=(ChannelContext)PoolManager.getInstance(ChannelContext.class);
		context.remoteIp=orgContext.remoteIp;
		context.remotePort=orgContext.remotePort;
		context.localIp=orgContext.localIp;
		context.localPort=orgContext.localPort;
		context.selectOperator.setup(null);
		context.writeOperator.setup(null);
		context.orderOperator.setup();
		return context;
	}
	
	/* spdy対応で実connectionがないContextのfinish */
	public synchronized void finishChildContext(){
		logger.debug("finishChildChannel.cid:"+getPoolId());
		orderOperator.doneClose(false);
		if(handler==null){//TODO なんてこった
			logger.warn("finishChildChannel.cid="+getPoolId()+":"+this);
			return;
		}
		orderOperator.checkAndCallbackFinish();
	}
	
	/* for debug */
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
		orderOperator.dump(logger);
		logger.debug("]");
	}
	/*
	@Override
	public void ref(){
		super.ref();
		logger.debug("#+#.cid:"+getPoolId(),new Throwable());
	}
	@Override
	public boolean unref(){
		logger.debug("#-#.cid:"+getPoolId(),new Throwable());
		return super.unref();
	}
	*/
	
}
