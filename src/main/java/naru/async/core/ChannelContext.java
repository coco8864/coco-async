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
import java.util.Set;
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
	
	public enum IO {
		IDLE,//IO���[�v�ɓ����Ă��Ȃ�
		QUEUE_SELECT,//select queue�݂̂��Q�Ɓ@->SELECT
		SELECT,//read�\�ɂȂ�̂�҂��Ă���@select�@thread�݂̂��Q�Ɓ@->IOST_QUEUE_READ or IOST_QUEUE_WRITE
//		QUEUE_IO,//read queue�݂̂��Q�� ->�@IOST_READ
		CONNECTABLE,
		READABLE,
		WRITABLE,
		CLOSEABLE,
		
//		IO,//read queue��read��������-> IOST_IDLE or IOST_QUEUE_SELECT
		CONNECTING,
		READING,
		WRITING,
		CLOSEING,
		
		CLOSED//��U���̃X�e�[�^�X�ɂȂ����ꍇ�Arecycle�܂Œl�̐ݒ�͕s��
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
//	private long writeOrderLength;
	private boolean isFinished;
	
	/**
	 * callbackOrders��ioLock��synchronized����\�������邪�A�������K�v�ȏꍇ�́A
	 * ioLock����synchronized����
	 */
	private LinkedList callbackOrders=new LinkedList();
	private boolean inCallback=false;
	
	private Object ioLock=new Object();
	private IO ioStatus=naru.async.core.ChannelContext.IO.IDLE;
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
//	private Set acceptIps;
	
	private static ChannelContext dummyContext=new ChannelContext();
	public static ChannelContext getDummyContext(){
		if(dummyContext==null){
			//�����ɗ�����2���̂͋��e����
			ChannelContext context=new ChannelContext();
			context.ioStatus=IO.CLOSED;
			context.isFinished=true;
			dummyContext=context;
		}
		return dummyContext;
	}
	
	public static ChannelContext create(ChannelHandler handler,SelectableChannel channel){
		ChannelContext context=(ChannelContext)PoolManager.getInstance(ChannelContext.class);
		context.setHandler(handler);
		context.channel=channel;
		context.readBuffer.setup();
		context.writeBuffer.setup();
		logger.debug("ChannelContext#create cid:"+context.getPoolId()+":ioStatus:"+context.ioStatus +":handler:"+handler.getPoolId()+":"+channel);
		if(channel instanceof SocketChannel){
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
	
	private void setIoStatus(IO ioStatus){
		if(logger.isDebugEnabled()){
			logger.debug("setIoStatus cid:"+getPoolId() + ":org:" +this.ioStatus+":new:"+ioStatus);
		}
		this.ioStatus=ioStatus;
	}
	
	public IO getIoStatus(){
		return ioStatus;
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
		setIoStatus(IO.IDLE);
		setHandler(null);
		connectTimeoutTime=Long.MAX_VALUE;
		readTimeoutTime=Long.MAX_VALUE;
		writeTimeoutTime=Long.MAX_VALUE;
//		readLength=writeLength=writeOrderLength=0;
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
	
	/*
	public boolean readable(){
		return isReadable;
	}
	public void readable(boolean isReadable){
		this.isReadable=isReadable;
	}
	public boolean writable(){
		return isWritable;
	}
	public void writable(boolean isWritable){
		this.isWritable=isWritable;
	}
	*/
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
		if(selector==null){
			selector=IOManager.getSelectorContext(this);
		}
//		logger.info("putSelector cid:"+getPoolId()+":selector:"+selector);
		selector.putContext(this);
		if(orders.orderCount()!=0){
			selector.wakeup();
		}
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
//			ChannelHandler orgHandler=this.handler;
			setHandler(handler);
//			orgHandler.unref();
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
			if(isFinished==false && ioStatus==IO.CLOSED){
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
			if(ioStatus==IO.CLOSED){
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
			if(writeLength==0){
				logger.warn("write buffer length 0.cid:"+getPoolId()+":buffers:"+buffers,new Throwable());
			}
			long asyncWriteLength=stastics.addAsyncWriteLength(writeLength);
//			writeOrderLength+=writeLength;
			logger.debug("writeOrder."+writeLength+":"+asyncWriteLength + ":cid:"+ getPoolId());
			writeOrder.setWriteEndOffset(asyncWriteLength);
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
//		if(handler==null){
//			logger.debug("setHandler(null) called.this:"+this,new Throwable());
//		}
		/*
		if(handler==null && this.handler!=null){
			logger.debug("setHandler(null) called.this:"+this,new Throwable());
			dump();
			logger.debug("setHandler(null) called.this.handler:"+this.handler);
			this.handler.dump();
		}
		*/
		/*
		if(handler!=null){
			handler.ref();
		}
		if(this.handler!=null){
			//unref����recycle�o�R�ł������Ăяo�����\������
			ChannelHandler tmpHandler=this.handler;
			this.handler=null;
			tmpHandler.unref();
		}
		*/
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
			if(ioStatus!=IO.CLOSED){
				setIoStatus(IO.IDLE);
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
			switch(ioStatus){
			case IDLE:
			case CONNECTING:
//			case READING:
//			case WRITING:
//			case CLOSEING:
				setIoStatus(IO.QUEUE_SELECT);
				putSelector();
				selector.wakeup();
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
				logger.debug("queueuSelect.cid:"+getPoolId()+":io:"+ioStatus);
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
			if(ioStatus!=IO.QUEUE_SELECT && ioStatus!=IO.SELECT ){
				if(selectionKey!=null){
					logger.debug("select#1 selectionKey.cancel().ioStatus:"+ioStatus);
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
				setIoStatus(IO.SELECT);//�Ȃ�����Ȃ̂��K�v�Ȃ̂��H
				queueIO(IO.CLOSEABLE);//�����Ō���IO.CLOSEABLE�ɂȂ�
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
			if(ioStatus==IO.SELECT){
				if(selectionKey.interestOps()!=ops){
					selectionKey.interestOps(ops);
				}
			}else if (ioStatus==IO.QUEUE_SELECT){
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
					channel.configureBlocking(false);
					selectionKey=selector.register(channel, ops,this);
					//TODO �����̂��߂�info
					//logger.debug("cid:"+getPoolId() +":ops:"+ops);
					setIoStatus(IO.SELECT);
				} catch (ClosedChannelException e) {
					logger.warn("select aleady closed.",e);
//					doneClosed(true);
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
	public boolean queueIO(IO io){
		synchronized(ioLock){
			if(ioStatus==IO.SELECT){
				if(selectionKey!=null){
					selectionKey.cancel();
					selectionKey=null;
				}
				setIoStatus(io);
				IOManager.enqueue(this);
				return true;
			}
			if(ioStatus==IO.READABLE){
				logger.debug("queueIO status IO.READABLE.cid:"+getPoolId()+":io:"+io);
				return false;
			}
			if(ioStatus==IO.WRITABLE && io==IO.WRITABLE){
				logger.debug("queueIO status IO.WRITABLE.cid:"+getPoolId()+":io:"+io);
				IOManager.enqueue(this);
				return true;
			}
		}
		return false;
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
			setIoStatus(IO.CLOSED);
			logger.warn("finishChannel.cid="+getPoolId()+":"+this);
			return;
		}
		handler.handlerClosed();//application����̖��߂�����
		//readOrder�������āAread���������Ă��邪�A�܂�readBuffer�ɒʒm���Ȃ��^�C�~���O������B
		//if( orders.isReadOrder()){
		//	readBuffer.waitForLastCallback();
		//}
		//�SOrder��closed�ŕԋp
		closable(false);
		Order finishOrder=Order.create(handler, Order.TYPE_FINISH, null);
		if(selectionKey!=null){
			selectionKey.cancel();
			selectionKey=null;
		}
		setIoStatus(IO.CLOSED);
		orders.closed(finishOrder);
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
						socket=null;
					}else if(serverSocket!=null){
						serverSocket.close();
						serverSocket=null;
					}
					
				}
			}else{
				logger.debug("close by order.cid:"+getPoolId()+":"+channel);
				if(socket!=null){
					//close�v���ŃN���[�Y����ꍇ�́A�n�[�t�N���[�Y
					if(socket.isConnected()){
						logger.debug("order getOutputStream.close().cid:"+getPoolId());
						socket.shutdownOutput();
//						socket.getOutputStream().close();
					}
				}else if(serverSocket!=null){
					if( !serverSocket.isClosed() ){
						serverSocket.close();
						serverSocket=null;
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
			serverSocket=null;
			socket=null;
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
		/*
		case black://black�ɂȂ���΋���
			if( matchPattern(blackList,clietnIp) ){
				return false;
			}
		case white://white�ɂȂ���΋���
			if( !matchPattern(whiteList,clietnIp) ){
				return false;
			}
		*/
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
//			ioStatus=IO_QUEUE_SELECT;//accept�����炻�̂܂�select���[�v�ɓ���
			/*ioStatus=IO_IDLE;*/
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
	
	public void prepareIO(IO io){
		synchronized(ioLock){
			setIoStatus(io);
		}
	}
	
	//������؂ꂽ�ꍇ�ɌĂяo�����A����ؒf��readOrder��ʂ��ăA�v���ɒʒm����
	public void disconnect(){
		synchronized(ioLock){
			setIoStatus(IO.CLOSED);
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
	
	/**
	 * ��Read���s����IOManager����Ăяo�����
	 * @param buffers
	 */
	public void doneRead(ByteBuffer[] buffers){
		stastics.addOnReadLength(BuffersUtil.remaining(buffers));
		synchronized(ioLock){
			logger.debug("doneRead.cid:"+getPoolId()+":"+ ioStatus);
			readBuffer.putBuffer(buffers);
			readBuffer.callback();
			//�ǂݍ��݊���
			readTimeoutTime=Long.MAX_VALUE;
			setIoStatus(IO.QUEUE_SELECT);
			putSelector();
			selector.wakeup();
		}
	}
	
	public ByteBuffer[] prepareWrite(){
		ByteBuffer[] buffers=writeBuffer.prepareWrite();
		synchronized(ioLock){
			if(buffers!=null){
				setIoStatus(IO.WRITING);
			}else{
				dump();
				//setIoStatus(IO.WAIT_WRITE_BUFFER);
				//queueuSelect();
			}
			return buffers;
		}
	}
	
	public void doneWrite(ByteBuffer[] prepareBuffers,long length){
		long writeLength=stastics.addWriteLength(length);
		logger.debug("doneWrite.cid:"+getPoolId()+":"+length+":"+writeLength + ":"+ ioStatus + ":"+ channel);
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
			setIoStatus(IO.QUEUE_SELECT);
			putSelector();
			selector.wakeup();
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
	
}
