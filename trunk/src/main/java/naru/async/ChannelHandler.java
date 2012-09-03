package naru.async;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import naru.async.core.ChannelContext;
import naru.async.core.Order;
import naru.async.core.SelectorContext;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public abstract class ChannelHandler extends PoolBase{
	private static Logger logger=Logger.getLogger(ChannelHandler.class);
	public static String CALLEE_HANDLER="naru.async.calleeHandler";
	//attrubuteのこのキーが設定されていた場合、このterminalにTraceBufferを通知する。
	public static String TraceBufferTreminal="naru.async.traceBufferTerminal";
	//上記が設定されていた時、TraceBufferのkeyにこの値を設定する。
	public static String TraceBufferKey="naru.async.traceBufferKey";
	
	public static int ORDER_ACCEPT=1;
	public static int ORDER_CONNECT=2;
	public static int ORDER_READ=3;
	public static int ORDER_WRITE=4;
	public static int ORDER_CLOSE=5;
	public static int ORDER_CANCEL=6;
	
	private ChannelContext context;
	private long totalReadLength=0;//contextが無くなった後,context情報を保持
	private long totalWriteLength=0;//contextが無くなった後,context情報を保持
	
//	private ChannelHandlerStastices stastics=new ChannelHandlerStastices();
	private boolean isClosed=false;//クローズを受け付けた後、次の要求を受けないため
	private Map attribute=new HashMap();//handlerに付随する属性
	
	public static class AcceptHandler extends ChannelHandler{
		public void onFinished() {
			logger.debug("#finished.cid:"+getChannelId());
		}
	}
	
	public void dump(){
		dump(logger);
	}
	
	public void dump(Logger logger){
		logger.debug("$cid:"+getChannelId() + ":poolId:"+getPoolId() +":className:" +getClass().getName());
		logger.debug("$attribute:"+attribute);
	}
	
	public String toString(){
		return "ChannelHandler[cid:"+getChannelId()+"]"+super.toString();
	}
	
	public void recycle() {
		Iterator itr=attribute.values().iterator();
		while(itr.hasNext()){
			Object value=itr.next();
			if(value instanceof PoolBase){
				PoolBase poolBase=(PoolBase)value;
				poolBase.unref();
			}
			itr.remove();
		}
		attribute.clear();
		setContext(null);
		isClosed=false;
		totalReadLength=totalWriteLength=0;
		super.recycle();
	}

	/**
	 * @param userContext onAcceptable,onAcceptedのuserContextとして通知されます。
	 * @param address
	 * @param backlog
	 * @param acceptClass
	 * @return
	 */
	public static ChannelHandler accept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass){
		return accept(userContext, address, backlog, acceptClass, IpBlockType.whiteBlack, null, null);
	}
	public static ChannelHandler accept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,IpBlockType ipBlockType,Pattern blackList,Pattern whiteList){
		if(!ChannelHandler.class.isAssignableFrom(acceptClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(AcceptHandler.class);
		if( handler.asyncAccept(userContext, address, backlog, acceptClass,ipBlockType,blackList,whiteList) ){
			return handler;
		}
		handler.unref(true);
		return null;
	}
	
	public static ChannelHandler connect(Class handlerClass,Object userContext,InetSocketAddress address,long timeout){
		if(!ChannelHandler.class.isAssignableFrom(handlerClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(handlerClass);
		if(handler.asyncConnect(userContext, address, timeout)){
			return handler;
		}
		handler.unref(true);
		return null;
	}
	
	private void setContext(ChannelContext context){
		if(context!=null){
			context.ref();
		}
		if(this.context!=null){
			totalReadLength=this.context.getTotalReadLength();			
			totalWriteLength=this.context.getTotalWriteLength();			
			logger.debug("setContext endHandler.cid:"+this.context.getPoolId()+":this:"+this+":newContext:"+context);
			this.context.unref();
		}
		this.context=context;
	}
	
	public long getReadTimeout() {
		return context.getReadTimeout();
	}

	public void setReadTimeout(long readTimeout) {
		context.setReadTimeout(readTimeout);
	}

	public long getWriteTimeout() {
		return context.getWriteTimeout();
	}

	public void setWriteTimeout(long writeTimeout) {
		context.setWriteTimeout(writeTimeout);
	}
	
	/* handler単位の属性 */
	public Object getHandlerAttribute(String name){
		return attribute.get(name);
	}
	
	public void setHandlerAttribute(String name, Object value) {
		if(name==SelectorContext.ATTR_ACCEPTED_CONTEXT){
			logger.debug("setContext() call.context"+value);
			//accept channelは、Selectorで作られる。context設定メソッドを隠蔽するための処理
			setContext((ChannelContext)value);
			return;
		}
		synchronized(attribute){
			attribute.put(name, value);
		}
	}
	
	public Iterator getHandlerAttributeNames(){
		return attribute.keySet().iterator();
	}
	
	/* channelContext単位の属性 */
	public Object getAttribute(String name){
		if(context==null){
			logger.error("getAttribute error.context is null.name:"+name+":" +this.getClass().getName(),new Exception());
			return null;
		}
		return context.getAttribute(name);
	}
	
	public void setAttribute(String name, Object value) {
		if(context==null){
			logger.error("setAttribute error.context is null.name:"+name+":value:"+value+":" +this.getClass().getName(),new Exception());
			return;
		}
		context.setAttribute(name, value);
	}
	
	public String getRemoteIp(){
		return	context.getRemoteIp();
	}
	
	public int getRemotePort(){
		return	context.getRemotePort();
	}
	
	public String getLocalIp(){
		return	context.getLocalIp();
	}
	
	public int getLocalPort(){
		return	context.getLocalPort();
	}
	
	public void handlerClosed(){
		if(isClosed==false){
			isClosed=true;
			dump();
			if(context!=null){
				context.dump();
			}
		}
	}
	public boolean isHandlerClosed(){
		return isClosed;
	}
	
	/**
	 * 回線の一意性をあらわすID,但し再起動すると同一番号が振られるのでグローバル一意ではない。
	 * @return
	 */
	public final long getChannelId(){
		if(context==null){
			return -1;
		}
		return context.getPoolId();
	}
	
	/**
	 * channelのhandlerを入れ替えるメソッド
	 * このメソッドを呼び出しが復帰後、このhandlerのメソッドを使ってはいけない。
	 * 新規に作られたhandlerのメソッドは呼び出し可
	 * @param handler
	 * @return
	 */
	public ChannelHandler forwardHandler(ChannelHandler handler){
		if(isClosed){
			logger.warn("fail to forwardHandler already closed handle.cid:"+getPoolId());
			handler.unref();//forwardに失敗したため、handlerは有効にならない
			return null;
		}
		if(context.foward(handler)==false){//Handler=Contextライフサイクルの同期を行う
			handler.unref();
			return null;//ない
		}
//		handler.attribute.putAll(attribute);//属性を引き継ぐ
//		attribute.clear();
		context.setHandler(handler);
		handler.setContext(context);
		setContext(null);
		handlerClosed();//自ハンドラは閉じたものとする
		unref();//自ハンドラは開放可能
		return handler;
	}
	
	public ChannelHandler forwardHandler(Class handlerClass){
		ChannelHandler handler=allocHandler(handlerClass);
		return forwardHandler(handler);
	}
	
	/**
	 * handlerを作成し呼び出しhandlerのライフサイクルと同期させる。
	 * @param handlerClass
	 * @return
	 */
	public ChannelHandler allocHandler(Class handlerClass){
		if(!ChannelHandler.class.isAssignableFrom(handlerClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(handlerClass);
		return handler;
	}
	
	/*
	 * 指定されたhandlerと呼び出しhandlerのライフサイクルと同期させる。
	 */
//	public ChannelHandler attachHandler(ChannelHandler handler){
//		return handler;
//	}

	public int orderCount(){
		return context.orderCount();
	}
	
	public enum IpBlockType{
//		black,//blackListを見てなければ許可
//		white,//whiteLsitを見てなければブロック
		blackWhite,//blackListを見てなければ、whiteLsitを見てなければブロック
		whiteBlack//whiteLsitを見てなければ、blackListを見てなければ許可
	}
	
	public boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass){
		return asyncAccept(userContext, address, backlog, acceptClass,IpBlockType.whiteBlack,null,null);
	}
	
	public boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,IpBlockType ipBlockType,Pattern blackList,Pattern whiteList){
		if(isClosed){
			return false;
		}
		/* contextを作る */
		ServerSocketChannel serverSocketChannel;
		try {
			serverSocketChannel = ServerSocketChannel.open();
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.socket().bind(address,backlog);
		} catch (IOException e) {
			logger.error("failt to asyncAccept.",e);
			return false;
		}
		setContext(ChannelContext.create(this, serverSocketChannel));
		Order order=Order.create(this, Order.TYPE_SELECT, userContext);
		context.setAcceptClass(acceptClass);
		context.setAcceptUserContext(userContext);
		context.setIpBlock(ipBlockType, blackList, whiteList);
		if(context.acceptOrder(order)==false){
			handlerClosed();
			context=ChannelContext.getDummyContext();
			return false;
		}
		return true;
	}
	

	public boolean asyncConnect(Object userContext,String remoteHost,int remotePort,long timeout){
		if(isClosed){
			logger.warn("fail to asyncConnect aleady closed.cid:"+getPoolId());
			return false;
		}
		InetAddress inetAddress=null;
		try {
			inetAddress = InetAddress.getByName(remoteHost);
		} catch (UnknownHostException e) {
			logger.warn("asyncConnect unknownHost.remoteHost:"+remoteHost);
			return false;
		}
		InetSocketAddress inetSocketAddress=new InetSocketAddress(inetAddress,remotePort);
		return asyncConnect(userContext,inetSocketAddress,timeout);
	}
	
	public boolean asyncConnect(Object userContext,InetSocketAddress address,long timeout){
		if(isClosed){
			logger.warn("fail to asyncConnect aleady closed.cid:"+getPoolId());
			return false;
		}
		SocketChannel channel;
		try {
			channel=SocketChannel.open();
			channel.configureBlocking(false);
			if(channel.connect(address)){
				//connectが完了しちゃった
			}
		} catch (IOException e) {
			logger.error("fail to connect",e);
			return false;
		}
		setContext(ChannelContext.create(this, channel));
		Order order=Order.create(this, Order.TYPE_CONNECT, userContext);
		if(context.connectOrder(order,timeout)==false){
			logger.warn("fail to asyncConnect connectOrder error.cid:"+getPoolId());
			context=ChannelContext.getDummyContext();
			return false;
		}
		return true;
	}

	private boolean order(Order order){
		if(context==null){
			logger.error("order error.this:"+this,new Exception());
			order.unref(true);
			return false;
		}
		if(context.order(order)==false){
			return false;
		}
		return true;
	}
	
	public boolean asyncRead(Object userContext){
		if(isClosed){
			logger.debug("fail to asyncRead aleady closed.cid:"+getChannelId());
			return false;
		}
		logger.debug("asyncRead.cid:"+getChannelId()+":userContext:"+userContext);
		Order order=Order.create(this, Order.TYPE_READ, userContext);
		if(context.readOrder(order)==false){
			logger.warn("fail to asyncRead readOrder error.cid:"+getChannelId());
			return false;
		}
		return true;
	}
	
	public boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
		if(isClosed){
			PoolManager.poolBufferInstance(buffers);//失敗した場合もbuffersは消費する
			logger.debug("fail to asyncWrite aleady closed.cid:"+getChannelId());
			return false;
		}
		logger.debug("asyncWrite.cid:"+getChannelId()+":this:"+this);
		if(logger.isDebugEnabled()){
			logger.debug("asyncWrite.cid:"+getChannelId()+":this:"+this +":length:"+BuffersUtil.remaining(buffers));
		}
		Order order=Order.create(this, Order.TYPE_WRITE, userContext,buffers);
		if(context.writeOrder(order)==false){
			logger.debug("fail to asyncWrite writeOrder error.id:"+getPoolId());
			return false;
		}
		return true;
	}
	
	public boolean asyncClose(Object userContext){
		if(isClosed){
			logger.debug("fail to asyncClose aleady closed.this:"+this);
			return false;
		}
		logger.debug("asyncClose.cid:"+getChannelId()+":this:"+this);
		if(context==null){
			logger.error("asyncClose context is null.this:"+this);
			return false;
		}
		Order order=Order.create(this, Order.TYPE_CLOSE, userContext);
		if(context.closeOrder(order)==false){
			logger.error("asyncClose closeOrder error.this:"+this);
			return false;
		}
		handlerClosed();
		return true;
	}
	
	public boolean asyncCancel(Object userContext){
		if(isClosed){
			logger.debug("fail to asyncCancel aleady closed.");
			return false;
		}
		logger.debug("asyncCancel.cid:"+getChannelId()+":this:"+this);
		Order order=Order.create(this, Order.TYPE_CANCEL, userContext);
		boolean result=order(order);
		handlerClosed();
		return result;
	}
	
	/* 出力をoutputStreamやwriter経由で指定するメソッド */
	private OutputStream outputStream;
	private Writer writer;
	private class HandlerOutputStream extends OutputStream{
		private ByteBuffer buffer;
		private int capacity;
		private int limit;
		private Object userContext;
		
		HandlerOutputStream(Object userContext){
			buffer=null;
			this.userContext=userContext;
		}
		
		public void close() throws IOException {
			outputStream=null;
			writer=null;
			flush();
		}

		public void flush() throws IOException {
			if(buffer!=null){
				buffer.flip();
//				System.out.println(new String(buffer.array(),0,buffer.limit()));
				asyncWrite(userContext,BuffersUtil.toByteBufferArray(buffer));
			}
			buffer=null;
		}

		public void write(byte[] src, int offset, int length) throws IOException {
			if(buffer!=null && capacity<(limit+length)){
				flush();
			}
			if(buffer==null){
				buffer=PoolManager.getBufferInstance();
				capacity=buffer.capacity();
				limit=0;
				//用意されたバッファより大きいデータがwriteされてしまった。
				if(capacity<length){
					PoolManager.poolBufferInstance(buffer);
					buffer=null;
					ByteBuffer[] buffers=BuffersUtil.buffers(src, offset, length);
					asyncWrite(userContext,buffers);
					return;
				}
			}
			buffer.put(src, offset, length);
			limit+=length;
		}

		public void write(byte[] src) throws IOException {
			write(src,0,src.length);
		}

		public void write(int src) throws IOException {
			write(new byte[]{(byte)src},0,1);
		}
	}
	
	public final OutputStream getOutputStream(Object userContext){
		if(outputStream!=null){
			return outputStream;
		}
		outputStream=new HandlerOutputStream(userContext);
		return outputStream;
	}
	
	public final Writer getWriter(Object userContext,String enc) throws UnsupportedEncodingException{
		if(writer!=null){
			return writer;
		}
		writer = new OutputStreamWriter(getOutputStream(userContext),enc);
		return writer;
	}
	
	/**
	 * asyncAcceptに対応,accept可能になった場合に呼ばれる
	 * @param userContext
	 */
	public void onAcceptable(Object userContext){
		logger.debug("#acceptable.cid:"+getChannelId());
	}
	
	/**
	 * accept完了時に、asyncAcceptのacceptClassに従ってhandlerが生成されこのメソッドが
	 * 呼び出される。userContextは、asyncAcceptで指定したオブジェクト
	 * @param userContext
	 */
	public void onAccepted(Object userContext){
		logger.debug("#accepted.cid:"+getChannelId());
	}
	
	public void onConnected(Object userContext){
		logger.debug("#connected.cid:"+getChannelId());
	}
	
	public void onRead(Object userContext,ByteBuffer[] buffers){
		logger.debug("#read.cid:"+getChannelId());
	}
	
	public void onWritten(Object userContext){
		logger.debug("#written.cid:"+getChannelId());
	}
	
	/**
	 * 要求に依存せず、回線がcloseしたことを通知
	 * Poolから取得したhandleは、再利用されるため、以降、このインスタンスを保持、メソッドを呼んではいけない。
	 */
	public abstract void onFinished();
	
	/**
	 * 要求が、回線がcloseにより実行できなかったたことを通知
	 * @param order
	 * @param userContexts
	 */
	public void onClosed(){
		logger.debug("#closed.cid:"+getChannelId());
	}
	public void onClosed(Object userContext){
		onClosed();
	}
	public void onWriteClosed(Object[] userContexts){
		onClosed(userContexts[0]);
	}
	public void onReadClosed(Object userContext){
		onClosed(userContext);
	}
	public void onAcceptClosed(Object userContext){
		onClosed(userContext);
	}
	public void onConnectClosed(Object userContext){
		onClosed(userContext);
	}
	//asyncClose処理でcloseされた場合...正常系で呼び出される
	public void onCloseClosed(Object userContext){
		onClosed(userContext);
	}
	public void onCancelClosed(Object userContext){
		onClosed(userContext);
	}
	
	/**
	 * 要求が、何らかの異常によりにより実行できなかったたことを通知
	 * @param order
	 * @param userContexts
	 * @param t
	 */
	public void onFailure(Throwable t){
		logger.debug("#failure.cid:"+getChannelId(),t);
		asyncClose(null);
	}
	public void onFailure(Object userContext,Throwable t){
		onFailure(t);
	}
	public void onWriteFailure(Object[] userContexts,Throwable t){
		if(userContexts==null){
			onFailure(null,t);
			return;
		}
		onFailure(userContexts[0],t);
	}
	public void onReadFailure(Object userContext,Throwable t){
		onFailure(userContext,t);
	}
	public void onAcceptFailure(Object userContext,Throwable t){
		onFailure(userContext,t);
	}
	public void onConnectFailure(Object userContext,Throwable t){
		onFailure(userContext,t);
	}
	public void onCloseFailure(Object userContext,Throwable t){
		onFailure(userContext,t);
	}
	public void onCancelFailure(Object userContext,Throwable t){
		onFailure(userContext,t);
	}
	
	/**
	 * 要求が、タイムアウトにより実行できなかった事を通知
	 * connect要求、read要求、write要求で発生
	 * @param order
	 * @param userContexts
	 */
	public void onTimeout() {
		logger.debug("#timeout.cid:"+getChannelId());
		asyncClose(null);
	}
	public void onTimeout(Object userContext) {
		onTimeout();
	}
	public void onWriteTimeout(Object[] userContexts){
		onTimeout(userContexts[0]);
	}
	public void onReadTimeout(Object userContext){
		onTimeout(userContext);
	}
	public void onConnectTimeout(Object userContext){
		onTimeout(userContext);
	}
	
	/**
	 * 要求が、asyncCancel要求により取り消された事を通知
	 * orderにORDER_CANCELが渡されてきたらcancel完了
	 * @param order
	 * @param userContexts
	 */
	public void onCanceled(){
		logger.debug("#canceled.cid:"+getChannelId());
	}
	public void onCanceled(Object userContext){
		onCanceled();
	}
	public void onWriteCanceled(Object[] userContexts){
		onCanceled(userContexts[0]);
	}
	public void onReadCanceled(Object userContext){
		onCanceled(userContext);
	}
	public void onAcceptCanceled(Object userContext){
		onCanceled(userContext);
	}
	public void onConnectCanceled(Object userContext){
		onCanceled(userContext);
	}
	//closeを先に処理する関係で発生しない
	public void onCloseCanceled(Object userContext){
		onCanceled(userContext);
	}
	//asyncCancel処理でcancelされた場合...正常系で呼び出される
	public void onCancelCanceled(Object userContext){
		onCanceled(userContext);
	}
	
	//contextがアプリケーションに通知した通算read長
	public long getTotalReadLength(){
		if(context==null){
			return totalReadLength;
		}
		return context.getTotalReadLength();
	}
	
	//contextがアプリケーションから受け取った通算write長
	public long getTotalWriteLength(){
		if(context==null){
			return totalWriteLength;
		}
		return context.getTotalWriteLength();
	}
	
	/* spdy用のcontextを作成 */
	public ChannelHandler allocChaildHandler(Class childHandlerClass){
		if(!ChannelHandler.class.isAssignableFrom(childHandlerClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelContext childContext=ChannelContext.childContext(context);
		ChannelHandler child=(ChannelHandler)PoolManager.getInstance(childHandlerClass);
		childContext.setHandler(child);
		child.setContext(childContext);
		return child;
	}
	
	public void finishChildHandler(){
		if(context==null){
			return;
		}
		context.finishChildContext();
	}
	
}
