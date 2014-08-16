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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import naru.async.core.ChannelContext;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public abstract class ChannelHandler extends PoolBase{
	private static Logger logger=Logger.getLogger(ChannelHandler.class);
	
	private ChannelContext context;
	private long totalReadLength=0;//contextが無くなった後,context情報を保持
	private long totalWriteLength=0;//contextが無くなった後,context情報を保持
	
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
		return accept(userContext, address, backlog, acceptClass, true, null, null);
	}
	public static ChannelHandler accept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,boolean isBlockOutOfList,Pattern blackList,Pattern whiteList){
		if(!ChannelHandler.class.isAssignableFrom(acceptClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(AcceptHandler.class);
		if( handler.asyncAccept(userContext, address, backlog, acceptClass,isBlockOutOfList,blackList,whiteList) ){
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
			totalWriteLength=this.context.getTotalReadLength();			
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
		if(context==null){
			logger.warn("fail to forwardHandler already closed handle.cid:"+getPoolId());
			handler.unref();//forwardに失敗したため、handlerは有効にならない
			return null;
		}
		if(context.foward(handler)==false){//Handler=Contextライフサイクルの同期を行う
			handler.unref();
			return null;//ない
		}
		handler.setContext(context);
		setContext(null);
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
	
	//isBlockOutOfList　whiteListに載ってない、けどbackListに載ってなければ許可
	public boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass){
		return asyncAccept(userContext, address, backlog, acceptClass,true,null,null);
	}
	/**
	 * 
	 * @param userContext
	 * @param address
	 * @param backlog
	 * @param acceptClass
	 * @param isBlockOutOfList whiteList,blackList両方に載ってないipを許可するか否か
	 * @param blackList
	 * @param whiteList
	 * @return
	 */
	public boolean asyncAccept(Object userContext,InetSocketAddress address,int backlog,Class acceptClass,boolean isBlockOutOfList,Pattern blackList,Pattern whiteList){
		ChannelContext serverContext=ChannelContext.serverChannelCreate(this, address,backlog);
		if(serverContext==null){
			return false;
		}
		setContext(serverContext);
		return serverContext.asyncAccept(userContext,address,backlog,acceptClass,isBlockOutOfList,blackList,whiteList);
	}

	public boolean asyncConnect(Object userContext,String remoteHost,int remotePort,long timeout){
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
		ChannelContext socketContext=ChannelContext.socketChannelCreate(this, address);
		if(socketContext==null){
			return false;
		}
		setContext(socketContext);
		return socketContext.asyncConnect(userContext, address, timeout);
	}
	
	public boolean asyncRead(Object userContext){
		if(context==null){
			logger.debug("fail to asyncRead aleady closed.cid:"+getChannelId());
			return false;
		}
		logger.debug("asyncRead.cid:"+getChannelId()+":userContext:"+userContext);
		return context.asyncRead(userContext);
	}
	
	public boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
		if(context==null){
			PoolManager.poolBufferInstance(buffers);//失敗した場合もbuffersは消費する
			logger.debug("fail to asyncWrite aleady closed.cid:"+getChannelId());
			return false;
		}
		logger.debug("asyncWrite.cid:"+getChannelId()+":this:"+this);
		if(logger.isDebugEnabled()){
			logger.debug("asyncWrite.cid:"+getChannelId()+":this:"+this +":length:"+BuffersUtil.remaining(buffers));
		}
		return context.asyncWrite(userContext, buffers);
	}
	
	public boolean asyncClose(Object userContext){
		if(context==null){
			logger.error("asyncClose context is null.this:"+this);
			return false;
		}
		logger.debug("asyncClose.cid:"+getChannelId()+":this:"+this);
		return context.asyncClose(userContext);
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
	
	public void onAcceptedInternal(ChannelContext context,Object userContext){
		if(this.context!=null){
			logger.debug("#onAcceptedInternal context is not null.cid:"+getChannelId());
			return;
		}
		setContext(context);
		onAccepted(userContext);
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
	
	/**
	 * 要求が、何らかの異常によりにより実行できなかったたことを通知
	 * @param order
	 * @param userContexts
	 * @param t
	 */
	public void onFailure(Throwable t){
		logger.debug("#failure.cid:"+getChannelId(),t);
		//asyncClose(null);
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
	
	/**
	 * 要求が、タイムアウトにより実行できなかった事を通知
	 * connect要求、read要求、write要求で発生
	 * @param order
	 * @param userContexts
	 */
	public void onTimeout() {
		logger.debug("#timeout.cid:"+getChannelId());
		//asyncClose(null);
	}
	public void onTimeout(Object userContext) {
		onTimeout();
	}
	public void onReadTimeout(Object userContext){
		onTimeout(userContext);
	}
	/* not occur */
	public void onWriteTimeout(Object[] userContexts){
		onTimeout(userContexts[0]);
	}
	/* not occur */
	public void onConnectTimeout(Object userContext){
		onTimeout(userContext);
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
