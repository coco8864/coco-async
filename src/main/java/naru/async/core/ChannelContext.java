package naru.async.core;

import java.io.IOException;
import java.net.InetAddress;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
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
	private static HashSet<ChannelContext> channelContexts=new HashSet<ChannelContext>();
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
	
	public enum SelectState {
		IDLE,//IO���[�v�ɓ����Ă��Ȃ�
		QUEUE_SELECT,//select queue�݂̂��Q�Ɓ@->SELECT
		SELECT,//read�\�ɂȂ�̂�҂��Ă���@select�@thread�݂̂��Q�Ɓ@->IOST_QUEUE_READ or IOST_QUEUE_WRITE
//		QUEUE_IO,//read queue�݂̂��Q�� ->�@IOST_READ
		CONNECTABLE,
		READABLE,
		//WRITABLE,
		//CLOSEABLE,
//		IO,//read queue��read��������-> IOST_IDLE or IOST_QUEUE_SELECT
		CONNECTING,
		READING,
		//WRITING,
		//CLOSEING,
		CLOSED//��U���̃X�e�[�^�X�ɂȂ����ꍇ�Arecycle�܂Œl�̐ݒ�͕s��
	}
	
	public enum WriteState{
		IDLE,
		WRITABLE,
		BLOCK,
		WRITING,
		CLOSEING,
		CLOSED
	}
	
	private ChannelStastics stastics=new ChannelStastics();
	
	private SelectableChannel channel;
	private Socket socket;
	private ServerSocket serverSocket;
	private String remoteIp=null;
	private int remotePort=-1;
	private String localIp=null;
	private int localPort=-1;
	
	private Map attribute=new HashMap();//handler�ɕt�����鑮��
	private SelectorContext selector;
	private SelectionKey selectionKey;//IO_SELECT�̏ꍇ�L��
	private ChannelHandler handler;
	private boolean isFinished;
	
	/**
	 * callbackOrders��ioLock��synchronized����\�������邪�A�������K�v�ȏꍇ�́A
	 * ioLock����synchronized����
	 */
	private LinkedList<Order> callbackOrders=new LinkedList<Order>();
	private boolean inCallback=false;
	
	private Object ioLock=new Object();
	private SelectState selectStatus=SelectState.IDLE;
	private WriteState writeStatus=WriteState.IDLE;
	
	private ContextOrders orders=new ContextOrders(this);
	private ReadBuffer readBuffer=new ReadBuffer(this);
	private WriteBuffer writeBuffer=new WriteBuffer(this);
	
	private long connectTimeoutTime=Long.MAX_VALUE;
	private long readTimeoutTime=Long.MAX_VALUE;
	private long writeTimeoutTime=Long.MAX_VALUE;
	
	private long readTimeout;
	private long writeTimeout;
	
//	private boolean isReadable=false;
//	private boolean isWritable=false;
	private boolean isClosable=false;
	
	private Class acceptClass;
	private Object acceptUserContext;
	private IpBlockType ipBlockType;
	private Pattern blackList;
	private Pattern whiteList;
	
	private static ChannelContext dummyContext=new ChannelContext();
	public static ChannelContext getDummyContext(){
		if(dummyContext==null){
			//�����ɗ�����2���̂͋��e����
			ChannelContext context=new ChannelContext();
			context.selectStatus=SelectState.CLOSED;
			context.isFinished=true;
			dummyContext=context;
		}
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
		logger.debug("ChannelContext#create cid:"+context.getPoolId()+":ioStatus:"+context.selectStatus +":handler:"+handler.getPoolId()+":"+channel);
		if(channel instanceof SocketChannel){
			context.setIoStatus(SelectState.SELECT);
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
	
	private void setIoStatus(SelectState ioStatus){
		if(logger.isDebugEnabled()){
			logger.debug("setIoStatus cid:"+getPoolId() + ":org:" +this.selectStatus+":new:"+ioStatus);
		}
		this.selectStatus=ioStatus;
	}
	
	public SelectState getIoStatus(){
		return selectStatus;
	}
	
	public Object getAttribute(String name){
		return attribute.get(name);
	}
	
	public void setAttribute(String name, Object value) {
		synchronized(attribute){
			attribute.put(name, value);
		}
	}
	
	public void recycle() {
		Iterator itr=attribute.values().iterator();
		while(itr.hasNext()){
			Object value=itr.next();
			if(value instanceof PoolBase){
				PoolBase poolBase=(PoolBase)value;
				poolBase.unref();
			}
		}
		attribute.clear();
		
		if(socket!=null){
			try {
				socket.close();
			} catch (IOException ignore) {
			}
			socket=null;
		}
		if(serverSocket!=null){
			try {
				serverSocket.close();
			} catch (IOException ignore) {
			}
			serverSocket=null;
		}
		remoteIp=localIp=null;
		remotePort=localPort=-1;
		setIoStatus(SelectState.IDLE);
		setHandler(null);
		connectTimeoutTime=Long.MAX_VALUE;
		readTimeoutTime=Long.MAX_VALUE;
		writeTimeoutTime=Long.MAX_VALUE;
		isFinished=false;
		selector=null;
		selectionKey=null;
		callbackOrders.clear();
		writeBuffer.recycle();
		readBuffer.recycle();
		orders.recycle();
		stastics.recycle();
		/*isReadable=isWritable=*/isClosable=false;
	}
	public void dump(){
		dump(logger);
	}
	
	public void dump(Logger logger){
		StringBuffer sb=new StringBuffer("[");
		sb.append("cid:");
		sb.append(getPoolId());
		sb.append(":selectStatus:");
		sb.append(selectStatus);
		sb.append(":writeStatus:");
		sb.append(writeStatus);
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
		writeBuffer.dump(logger);
		readBuffer.dump(logger);
		orders.dump(logger);
		logger.debug("]");
	}
	
	public SelectableChannel getChannel(){
		return channel;
	}
	
	public final String getRemoteIp(){
		return	remoteIp;
	}
	
	public final int getRemotePort(){
		return	remotePort;
	}
	
	public final String getLocalIp(){
		return	localIp;
	}
	
	public final int getLocalPort(){
		return	localPort;
	}
	
	public boolean closable(){
		return isClosable;
	}
	public void closable(boolean isClosable){
		this.isClosable=isClosable;
	}
	
	public boolean isConnected(){
		return socket.isConnected();
	}
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
	
	private void putSelector(){
//		logger.info("putSelector cid:"+getPoolId()+":selector:"+selector);
		selector.putContext(this);
		selector.wakeup();
	}
	
	/**
	 * ioLock synchronized�̒�����Ăяo����
	 * @return
	 */
	private int operations(){
		return orders.operations();
	}
	
	/* callback�֘A */
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
	
	private static ThreadLocal<ChannelContext> currentContext=new ThreadLocal<ChannelContext>();
	/**
	 * dispatch worker����Ăяo�����A���d�ł͑��s���Ȃ�
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
					//������ؒf���ꂽ�Ƃ��ď���
					doneClosed(true);
				}
			}
		}finally{
			currentContext.set(null);
			if(isFinishCallback){
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
	}
	
	public int orderCount(){
		synchronized(ioLock){
			synchronized(callbackOrders){
				int count=callbackOrders.size()+orders.orderCount();
				if( currentContext.get()==this ){
					return count-1;
				}else{
					return count;
				}
			}
		}
	}
	
	public boolean foward(ChannelHandler handler){
		synchronized(ioLock){
			int orderCount=orderCount();
			if(orderCount!=0){
				logger.debug("left order foward.orderCount:"+orderCount +":handler:" + handler);
			}
			setHandler(handler);
		}
		return true;
	}
	
	/**
	 * handler����̃��N�G�X�g����t��
	 * asyncAccept��asyncCancel��asyncClose����Ăяo�����
	 * @param order
	 * @return
	 */
	public boolean order(Order order){
		synchronized(ioLock){
			if(isFinished){
				order.unref();
				return false;
			}
			if(orders.order(order)==false){
				order.unref();
				return false;
			}
			queueuSelect();
			return true;
		}
	}
	
	public boolean closeOrder(Order order){
		synchronized(ioLock){
			if(order(order)==false){
				return false;
			}
			//������ؒf����Ă��āA����readBuffer�Ƀf�[�^���c���Ă����ꍇ�Acallback�����_�@���Ȃ��Ȃ�
			//readBuffer���Ȃ��ꍇ�́Afinish���Ăяo�����
			if(isFinished==false && selectStatus==SelectState.CLOSED){
				finishChannel();
			}
		}
		return true;
	}
	
	
	public boolean acceptOrder(Order order){
		if(order(order)){
			stastics.asyncAccept();
			return true;
		}
		return false;
	}
	
	public boolean connectOrder(Order connectOrder,long timeout){
		synchronized(ioLock){
			if(isFinished){
				return false;
			}
			if(orders.order(connectOrder)){
				stastics.asyncConnect();		
				if(timeout!=0){
					connectTimeoutTime=System.currentTimeMillis()+timeout;
				}
				queueuSelect();
				return true;
			}
		}
		return false;
	}
	
	public boolean writeOrder(Order writeOrder){
		synchronized(ioLock){
			if(isFinished){
				logger.debug("writeOrder aleady finished.cid:"+getPoolId());
				writeOrder.unref(true);
				return false;
			}
			if(selectStatus==SelectState.CLOSED){
				logger.debug("writeOrder aleady CLOSED.cid:"+getPoolId());
				writeOrder.unref(true);
				return false;
			}
			if(!orders.order(writeOrder)){
				writeOrder.unref(true);
				return false;
			}
			ByteBuffer[] buffers=writeOrder.popBuffers();
			
			long writeLength=BuffersUtil.remaining(buffers);
			long asyncWriteLength=stastics.addAsyncWriteLength(writeLength);
			logger.debug("writeOrder."+writeLength+":"+asyncWriteLength + ":cid:"+ getPoolId());
			writeOrder.setWriteEndOffset(asyncWriteLength);
			if(writeLength==0){
				//�������ݒ���0�̏ꍇ�Awrite���������Ȃ��̂ł�����callback���˗�����
				long totalWriteLength=stastics.getWriteLength();
				if(asyncWriteLength==totalWriteLength){
					orders.doneWrite(totalWriteLength);
				}
				PoolManager.poolBufferInstance(buffers);
				return true;
			}
			writeBuffer.putBuffer(buffers);
			if( writeTimeout!=0 && writeTimeoutTime==Long.MAX_VALUE){
				writeTimeoutTime=System.currentTimeMillis()+writeTimeout;
			}
			/* �����Ɉȉ����R�����g�A�E�g onBuffer�ɂ���̂ŕK�v�Ȃ��񂶂�Ȃ����H */
			//queueuSelect();
			return true;
		}
	}
	
	public boolean readOrder(Order readOrder){
		synchronized(ioLock){
			if(isFinished){
				readOrder.unref(true);
				return false;
			}
			if(orders.order(readOrder)){
				stastics.asyncRead();
				if(readBuffer.callback()==false){
					//read order������̂ɁAcallback���Ă��Ȃ� -> �f�[�^���������Ă��Ȃ�
					if( readTimeout!=0 ){
						//TODO readTimeoutTime��ݒ肷��̂ł͂Ȃ��A���莞��timeout�������v�Z����
						readTimeoutTime=System.currentTimeMillis()+readTimeout;
					}
					queueuSelect();
				}
				return true;
			}
		}
		readOrder.unref(true);
		return false;
	}
	
	//readBuffer��swapin���Ă����ꍇ
	public boolean onSwapinReadBuffer(){
		synchronized(ioLock){
			return readBuffer.callback();
		}
	}
	
	public void setConnectTimeoutTime(long connectTimeoutTime){
		this.connectTimeoutTime=connectTimeoutTime;
	}
	
	public void setHandler(ChannelHandler handler){
		if(handler==null){
			logger.debug("setHandler(null) cid:" +getPoolId()+":orgHandler:"+ this.handler);
		}
		this.handler=handler;
	}
	
	public void setAcceptClass(Class acceptClass){
		this.acceptClass=acceptClass;
	}
	public Class getAcceptClass(){
		return acceptClass;
	}
	
	public void setAcceptUserContext(Object acceptUserContext){
		this.acceptUserContext=acceptUserContext;
	}
	public Object getAcceptUserContext(){
		return acceptUserContext;
	}
	
	public void setIpBlock(IpBlockType ipBlockType,Pattern blackList,Pattern whiteList){
		this.ipBlockType=ipBlockType;
		this.blackList=blackList;
		this.whiteList=whiteList;
	}
	
	/**
	 * close�\�̏ꍇtrue��ԋp
	 * @param e
	 * @return
	 */
	public boolean failure(Throwable e){
		synchronized(ioLock){
			//���󂯕t���Ă���close�ȊO��order�����ׂ�failure��callback queue��
			orders.failure(e);
			if(selectStatus!=SelectState.CLOSED){
				setIoStatus(SelectState.IDLE);
			}
			if(orders.isCloseable()){
				return true;
			}
			return false;
		}
	}
	
	/**
	 * IO���J�n�������󋵂̏ꍇ�Ăяo��
	 */
	public void queueuSelect(){
		synchronized(ioLock){
			switch(selectStatus){
			case IDLE:
			case CONNECTING:
//			case READING:
//			case WRITING:
//			case CLOSEING:
				setIoStatus(SelectState.QUEUE_SELECT);
				selector.putContext(this);
				break;
			case QUEUE_SELECT:
			case SELECT:
				selector.wakeup();
				break;
			case CLOSED:
				logger.debug("disconnect and close request ioStatus==IO.CLOSED.cid:"+getPoolId());
				readBuffer.callback();
				break;
			default:
				logger.debug("queueuSelect.cid:"+getPoolId()+":io:"+selectStatus);
			}
		}
	}
	
	/**
	 * select����������(�ȉ��Q�̃p�^�[��)�����ꍇ
	 * 1)selector����폜���ꂽ nextWakeup
	 * 2)selector�ɎQ�������� ����̃^�C���A�E�g����
	 * selector�ɎQ��������Ƃ������A�����ł��Ȃ������ꍇ -1
	 * @param selector
	 * @return�@����^�C���A�E�g����
	 */
	public long select(SelectorContext selector,long nextWakeup){
		long now=System.currentTimeMillis();
		synchronized(ioLock){
			if(selectStatus!=SelectState.QUEUE_SELECT && selectStatus!=SelectState.SELECT ){
				if(selectionKey!=null){
					logger.debug("select#1 selectionKey.cancel().ioStatus:"+selectStatus);
					selectionKey.cancel();
					selectionKey=null;
				}
				return nextWakeup;
			}
			/* close�v�����������Ă�����A�o�͂���� */
			if(orders.isCloseable()){
				if(selectionKey!=null){
					logger.debug("select#2 selectionKey.cancel().");
					selectionKey.cancel();
					selectionKey=null;
				}
				closable(true);
				setIoStatus(SelectState.SELECT);//�Ȃ�����Ȃ̂��K�v�Ȃ̂��H
				queueIO(SelectState.CLOSEABLE);//�����Ō���IO.CLOSEABLE�ɂȂ�
				return nextWakeup;
			}
			/* cansel�v��? */
			if(orders.isCancelOrder()){
				orders.cancel();
			}
			/* timeout���� */
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
			if(selectStatus==SelectState.SELECT){
				if(selectionKey.interestOps()!=ops){
					selectionKey.interestOps(ops);
				}
			}else if (selectStatus==SelectState.QUEUE_SELECT){
				try {
					if(channel.isRegistered()){
						logger.debug("IO_QUEUE_SELECT and isRegistered.cid:" + getPoolId() +":selectionKey:"+selectionKey);
//						logger.info("####cid:" +getPoolId() + " selector:"+selector + " isRegistered:" + channel.isRegistered());
						//�V�K��selector�ɎQ�������悤�Ƃ������A���łɎQ�����Ă���?
						//selectionKey=channel.keyFor(selector);
						//�O��IO�\�ɂȂ��Ă���AIO�������U�ŏI������̂ŁA�O��IO�Aselector���܂�select�ɓ����Ă��Ȃ�
						//SELECT�ɂȂ邱�Ƃ��ł��Ȃ��B�P��x�݁B
						//�z�肵������ł�����Ȃ�
						if(selectionKey!=null){
							logger.debug("cid:"+getPoolId() +":select#3 selectionKey.cancel().");
							selectionKey.cancel();
							selectionKey=null;
						}
						return -1;
					}
					//������close����Ă���ƈȉ��̗�O
					/*
2011-11-24 11:32:11,462 [selector-2] WARN  naru.async.core.ChannelContext - select aleady closed.
java.nio.channels.ClosedChannelException
        at java.nio.channels.spi.AbstractSelectableChannel.configureBlocking(AbstractSelectableChannel.java:252)
        at naru.async.core.ChannelContext.select(ChannelContext.java:736)
        at naru.async.core.SelectorContext.selectAll(SelectorContext.java:134)
					 */
					channel.configureBlocking(false);
					selectionKey=selector.register(channel, ops,this);
					//TODO �����̂��߂�info
					//logger.debug("cid:"+getPoolId() +":ops:"+ops);
					setIoStatus(SelectState.SELECT);
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
	
	/**
	 * isForce��true�̏ꍇ�A����status�Ɋ֌W�Ȃ��AIO.QUEUE_IO�ɂ���enqueue����
	 * @param isForce
	 */
	public boolean queueIO(SelectState io){
		synchronized(ioLock){
			if(selectStatus==SelectState.SELECT){
				if(selectionKey!=null){
					selectionKey.cancel();
					selectionKey=null;
				}
				setIoStatus(io);
				IOManager.enqueue(this);
				return true;
			}
			if(selectStatus==SelectState.READABLE){
				logger.debug("queueIO status IO.READABLE.cid:"+getPoolId()+":io:"+io);
				return false;
			}
			if(selectStatus==SelectState.WRITABLE && io==SelectState.WRITABLE){
				logger.debug("queueIO status IO.WRITABLE.cid:"+getPoolId()+":io:"+io);
				IOManager.enqueue(this);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 
	 * @param isReal true:0������M���ă����[�g����؂�ꂽ�ꍇ�ɌĂяo�����B
	 */
	public void doneClosed(boolean isReal){
		logger.debug("doneClosed cid:"+getPoolId() + ":isReal:" +isReal);
		try {
			if(isReal){
				logger.debug("close by read 0."+ channel);
				if(channel.isOpen()){
					logger.debug("isReal channel.close().cid:"+getPoolId());
					channel.close();
					if(socket!=null){
						socket.close();
					}else if(serverSocket!=null){
						serverSocket.close();
					}
					
				}
			}else{
				logger.debug("close by order.cid:"+getPoolId()+":"+channel);
				if(socket!=null){
					//close�v���ŃN���[�Y����ꍇ�́A�n�[�t�N���[�Y
					if(socket.isConnected()){
						logger.debug("order getOutputStream.close().cid:"+getPoolId());
						socket.shutdownOutput();
					}
				}else if(serverSocket!=null){
					if( !serverSocket.isClosed() ){
						serverSocket.close();
					}
				}else{
					if(channel!=null && channel.isOpen()){
						logger.debug("order channel.close().cid:"+getPoolId());
						channel.close();
					}
				}
			}
		} catch (IOException e) {
			//peer����؂���Ȃ�close�����s���邱�Ƃ͂���Aclose�Ɏ��s���Ă��㏈���͂Ȃ�
			logger.debug("close erroe.",e);
		}
		finishChannel();
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
	
	public boolean acceptable(Socket socket){
		String clietnIp=socket.getInetAddress().getHostAddress();
		switch(ipBlockType){
		case blackWhite://black�����ĂȂ����white�����ĂȂ���΋���
			if( matchPattern(blackList,clietnIp) ){
				return false;
			}
			if( !matchPattern(whiteList,clietnIp) ){
				return false;
			}
		case whiteBlack://white�����ĂȂ����black�����ĂȂ���΋���
			if( !matchPattern(whiteList,clietnIp) ){
				if( matchPattern(blackList,clietnIp) ){
					return false;
				}
			}
		}
		Order order=Order.create(handler, Order.TYPE_SELECT, acceptUserContext);
		synchronized(ioLock){
			queueCallback(order);
		}
		return true;
	}
	public void accepted(Object userContext){
		if(handler==null){
			logger.error("accepted error handler=null.this:"+toString());
			return;
		}
		Order order=Order.create(handler, Order.TYPE_ACCEPT, userContext);
		synchronized(ioLock){
			queueCallback(order);
		}
	}
	
	public void finishConnect() throws IOException{
		((SocketChannel)channel).finishConnect();//�����[�g���~�܂��Ă���ꍇ�́A������ConnectException�ƂȂ�B
		synchronized(ioLock){
			connectTimeoutTime=Long.MAX_VALUE;
			orders.doneConnect();
		}
	}
	
	public void prepareIO(SelectState io){
		synchronized(ioLock){
			setIoStatus(io);
		}
	}
	
	//������؂ꂽ�ꍇ�ɌĂяo�����A����ؒf��readOrder��ʂ��ăA�v���ɒʒm����
	public void disconnect(){
		synchronized(ioLock){
			setIoStatus(SelectState.CLOSED);
		}
		//�u�ԓI��
		readBuffer.disconnect();
		readBuffer.callback();
		if(orders.isCloseable()){
			doneClosed(false);
		}
	}
	
	/**
	 * ReadBuffer����Ăяo�����
	 * @param buffers
	 */
	boolean ordersDoneRead(ByteBuffer[] buffers){
		return orders.doneRead(buffers);
	}
	
	
	
	/* read io�̒��O�ɌĂяo����� */
	public void prepareRead(){
		synchronized(ioLock){
			if(selectStatus==SelectState.READABLE){
				selectStatus=SelectState.READING;
			}else{
				throw new RuntimeException("selectStatus error:"+selectStatus);
			}
		}
	}
	
	/**
	 * ��Read���s����IOManager����Ăяo�����
	 * read io�̒���ɂ�т������
	 * @param buffers
	 */
	public void doneRead(ByteBuffer[] buffers){
		stastics.addOnReadLength(BuffersUtil.remaining(buffers));
		synchronized(ioLock){
			logger.debug("doneRead.cid:"+getPoolId()+":"+ selectStatus);
			readBuffer.putBuffer(buffers);
			//�ǂݍ��݊���
			readTimeoutTime=Long.MAX_VALUE;
			if(selectStatus==SelectState.READING){
				if(buffers==null){//0��read
					selectStatus=SelectState.CLOSED;
				}else{
					selectStatus=SelectState.QUEUE_SELECT;
					selector.putContext(this);
				}
			}else{
				throw new RuntimeException("selectStatus error:"+selectStatus);
			}
		}
	}
	
	/* write io�̒��O�ɌĂяo����� */
	public ByteBuffer[] prepareWrite(){
		ByteBuffer[] buffers=writeBuffer.prepareWrite();
		synchronized(ioLock){
			if(writeStatus==WriteState.WRITABLE){
				writeStatus=WriteState.WRITING;
			}else{
				throw new RuntimeException("writeStateus error:"+writeStatus);
			}
			return buffers;
		}
	}
	
	/* write io�̒���ɌĂяo����� */
	public void doneWrite(ByteBuffer[] prepareBuffers,long length){
		long writeLength=stastics.addWriteLength(length);
		long lestLength=BuffersUtil.remaining(prepareBuffers);
		logger.debug("doneWrite.cid:"+getPoolId()+":"+length+":"+writeLength + ":"+ selectStatus + ":"+ channel);
		writeBuffer.doneWrite(prepareBuffers);
		synchronized(ioLock){
			//callback�\�ɂȂ�����=>putCallbackOrder()
			orders.doneWrite(writeLength);
			if( orders.isWriteOrder()){
				if(writeTimeout!=0){
					writeTimeoutTime=System.currentTimeMillis()+writeTimeout;
				}
			}else{
				writeTimeoutTime=Long.MAX_VALUE;
			}
			if(writeStatus==WriteState.WRITING){
				if(lestLength==0){
					writeStatus=WriteState.WRITABLE;
				}else{
					writeStatus=WriteState.BLOCK;
				}
			}else{
				throw new RuntimeException("writeStateus error:"+writeStatus);
			}
		}
	}
	
	//�A�v���P�[�V�����ɒʒm�����ʎZread��
	public long getTotalReadLength(){
		return readBuffer.getOnBufferLength();
	}
	
	//�A�v���P�[�V��������󂯎�����ʎZwrite��,��write�Ƃ͎኱���ق�����
	public long getTotalWriteLength(){
		return writeBuffer.getPutLength();
	}
	
	private void finishChannel(){
		logger.debug("finishChannel.cid:"+getPoolId());
		//�قړ����ɂQthread����Ăяo����鎖������A���̏ꍇ�A���s����handler��null�ɂȂ�
		synchronized(ioLock){
			if(isFinished){//����finish���Ăт����Ă���
				logger.debug("aleady call finishChannel.cid:"+getPoolId());
				return;
			}
			isFinished=true;//�ȍ~Order���󂯂��Ȃ�
		}
		if(handler==null){//TODO �Ȃ�Ă�����
			setIoStatus(SelectState.CLOSED);
			logger.warn("finishChannel.cid="+getPoolId()+":"+this);
			return;
		}
		handler.handlerClosed();//application����̖��߂�����
		closable(false);
		Order finishOrder=Order.create(handler, Order.TYPE_FINISH, null);
		if(selectionKey!=null){
			selectionKey.cancel();
			selectionKey=null;
		}
		setIoStatus(SelectState.CLOSED);
		orders.closed(finishOrder);
	}
	
	/* spdy�Ή��Ŏ�connection���Ȃ�Context��finish */
	public void finishChildContext(){
		logger.debug("finishChildChannel.cid:"+getPoolId());
		//�قړ����ɂQthread����Ăяo����鎖������A���̏ꍇ�A���s����handler��null�ɂȂ�
		synchronized(ioLock){
			if(isFinished){//����finish���Ăт����Ă���
				logger.debug("aleady call finishChildChannel.cid:"+getPoolId());
				return;
			}
			isFinished=true;//�ȍ~Order���󂯂��Ȃ�
		}
		setIoStatus(SelectState.CLOSED);
		if(handler==null){//TODO �Ȃ�Ă�����
			logger.warn("finishChildChannel.cid="+getPoolId()+":"+this);
			return;
		}
		Order finishOrder=Order.create(handler, Order.TYPE_FINISH, null);
		orders.closed(finishOrder);
	}
	
	/*
	@Override
	public void ref() {
		logger.info("###ref cid:" +getPoolId(),new Throwable());
		super.ref();
	}

	@Override
	public boolean unref() {
		logger.info("###unref cid:" +getPoolId(),new Throwable());
		return super.unref();
	}

	@Override
	public boolean unref(boolean isPool) {
		logger.info("###unref2 cid:" +getPoolId(),new Throwable());
		return super.unref(isPool);
	}
	*/
}
