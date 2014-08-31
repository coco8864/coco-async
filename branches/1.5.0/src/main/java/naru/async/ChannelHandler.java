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
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import naru.async.core.ChannelContext;
import naru.async.pool.BuffersUtil;
import naru.async.pool.Context;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public abstract class ChannelHandler extends Context{
	private static Logger logger=Logger.getLogger(ChannelHandler.class);
	public enum State {
		init,
		connect,
		close,
		forwarded,
		finished
	}
	private State state;
	private Object lock=new Object();
	
	private ChannelContext context;
	private long totalReadLength=0;//contextが無くなった後,context情報を保持
	private long totalWriteLength=0;//contextが無くなった後,context情報を保持
	
	public static class AcceptHandler extends ChannelHandler{
		public void onFinished() {
			logger.debug("AcceptHandler#finished.cid:"+getChannelId());
		}
	}
	
	public void recycle() {
		state=State.init;
		setContext(null);
		totalReadLength=totalWriteLength=0;
		super.recycle();
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
			synchronized(lock){
				if(context==null&&this.state==State.connect){
					this.state=State.finished;
				}
			}
		}
		this.context=context;
	}
	
	/**
	 * handlerを作成し呼び出しhandlerのライフサイクルと同期させる。
	 * @param handlerClass
	 * @return
	 */
	public static ChannelHandler allocHandler(Class handlerClass){
		if(!ChannelHandler.class.isAssignableFrom(handlerClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(handlerClass);
		return handler;
	}

	/**
	 * @param acceptClass
	 * @param address
	 * @param backlog
	 * @param userContext onAcceptable,onAcceptedのuserContextとして通知されます。
	 * @return
	 */
	public static ChannelHandler accept(Class acceptHandlerClass,InetSocketAddress address,int backlog,Object userContext){
		return accept(acceptHandlerClass, address, backlog, true, null, null, userContext);
	}
	
	public static ChannelHandler accept(Class acceptHandlerClass,InetSocketAddress address,int backlog,boolean isBlockOutOfList,Pattern blackList,Pattern whiteList,Object userContext){
		if(!ChannelHandler.class.isAssignableFrom(acceptHandlerClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(AcceptHandler.class);
		if( handler.asyncAccept(acceptHandlerClass, address, backlog, isBlockOutOfList,blackList,whiteList,userContext) ){
			return handler;
		}
		handler.unref(true);
		return null;
	}
	
	public static ChannelHandler connect(Class handlerClass,InetSocketAddress address,long timeout,Object userContext){
		if(!ChannelHandler.class.isAssignableFrom(handlerClass)){
			throw new IllegalArgumentException("illegal handlerClass");
		}
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(handlerClass);
		if(handler.asyncConnect(address, timeout, userContext)){
			return handler;
		}
		handler.unref(true);
		return null;
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
	
	/* channelContext単位の属性 */
	public Object getChannelAttribute(String name){
		if(context==null){
			logger.error("getAttribute error.context is null.name:"+name+":" +this.getClass().getName(),new Exception());
			return null;
		}
		return context.getAttribute(name);
	}
	
	public void setChannelAttribute(String name, Object value) {
		if(context==null){
			logger.error("setAttribute error.context is null.name:"+name+":value:"+value+":" +this.getClass().getName(),new Exception());
			return;
		}
		context.setAttribute(name, value);
	}
	
	public void endowChannelAttribute(String name, PoolBase value) {
		if(context==null){
			logger.error("setAttribute error.context is null.name:"+name+":value:"+value+":" +this.getClass().getName(),new Exception());
			return;
		}
		context.endowAttribute(name, value);
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
		if(state!=State.connect){
			logger.warn("fail to forwardHandler already closed handle.cid:"+getPoolId()+":"+state);
			handler.unref();//forwardに失敗したため、handlerは有効にならない
			return null;
		}
		context.foward(handler);//Handler=Contextライフサイクルの同期を行う
		handler.setContext(context);
		setContext(null);
		state=State.forwarded;
		handler.state=State.connect;
		unref();//自ハンドラは開放可能
		return handler;
	}
	
	public ChannelHandler forwardHandler(Class handlerClass){
		ChannelHandler handler=allocHandler(handlerClass);
		if(forwardHandler(handler)==null){
			handler.unref();
			handler=null;
		}
		return handler;
	}
	
	//isBlockOutOfList　whiteListに載ってない、けどbackListに載ってなければ許可
	public boolean asyncAccept(InetSocketAddress address,int backlog,Class acceptClass,Object userContext){
		return asyncAccept(acceptClass, address, backlog, true,null,null,userContext);
	}
	/**
	 * 
	 * @param acceptClass
	 * @param address
	 * @param backlog
	 * @param isBlockOutOfList whiteList,blackList両方に載ってないipを許可するか否か
	 * @param blackList
	 * @param whiteList
	 * @param userContext
	 * @return
	 */
	public boolean asyncAccept(Class acceptHandlerClass,InetSocketAddress address,int backlog,boolean isBlockOutOfList,Pattern blackList,Pattern whiteList,Object userContext){
		ChannelContext serverContext=null;
		synchronized(lock){
			if(state!=State.init){
				logger.debug("asyncAccept error state:"+state +":cid:"+getChannelId());
				return false;
			}
			serverContext=ChannelContext.serverChannelCreate(this, address,backlog);
			if(serverContext==null){
				return false;
			}
			setContext(serverContext);
			state=State.connect;
		}
		return serverContext.asyncAccept(acceptHandlerClass,address,backlog,isBlockOutOfList,blackList,whiteList,userContext);
	}

	public boolean asyncConnect(String remoteHost,int remotePort,long timeout,Object userContext){
		InetAddress inetAddress=null;
		try {
			inetAddress = InetAddress.getByName(remoteHost);
		} catch (UnknownHostException e) {
			logger.warn("asyncConnect unknownHost.remoteHost:"+remoteHost);
			return false;
		}
		InetSocketAddress inetSocketAddress=new InetSocketAddress(inetAddress,remotePort);
		return asyncConnect(inetSocketAddress,timeout,userContext);
	}
	
	public boolean asyncConnect(InetSocketAddress address,long timeout,Object userContext){
		ChannelContext socketContext=null;
		synchronized(lock){
			if(state!=State.init){
				logger.debug("asyncConnect error state:"+state +":cid:"+getChannelId());
				return false;
			}
			socketContext=ChannelContext.socketChannelCreate(this, address);
			if(socketContext==null){
				return false;
			}
			setContext(socketContext);
			state=State.connect;
		}
		return socketContext.asyncConnect(address, timeout, userContext);
	}
	
	public boolean asyncRead(Object userContext){
		if(state!=State.connect){
			logger.debug("asyncRead error state:"+state +":cid:"+getChannelId());
			return false;
		}
		logger.debug("asyncRead.cid:"+getChannelId()+":userContext:"+userContext);
		return context.asyncRead(userContext);
	}
	
	public boolean asyncWrite(ByteBuffer[] buffers,Object userContext){
		synchronized(lock){
			if(state!=State.connect){
				PoolManager.poolBufferInstance(buffers);//失敗した場合もbuffersは消費する
				logger.debug("asyncWrite error state:"+state +":cid:"+getChannelId());
				return false;
			}
		}
		logger.debug("asyncWrite.cid:"+getChannelId()+":this:"+this);
		if(logger.isDebugEnabled()){
			logger.debug("asyncWrite.cid:"+getChannelId()+":length:"+BuffersUtil.remaining(buffers));
		}
		return context.asyncWrite(buffers, userContext);
	}
	
	public boolean asyncClose(Object userContext){
		synchronized(lock){
			if(state!=State.connect){
				logger.debug("asyncClose error state:"+state +":cid:"+getChannelId());
				return false;
			}
			state=State.close;
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
				asyncWrite(BuffersUtil.toByteBufferArray(buffer),userContext);
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
					asyncWrite(buffers,userContext);
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
	
	public final void onAcceptedInternal(ChannelContext context,Object userContext){
		if(state!=State.init){
			logger.debug("#onAcceptedInternal context is not null.cid:"+context.getPoolId());
		}
		state=State.connect;
		setContext(context);
		onAccepted(userContext);
	}
	
	public void onConnected(Object userContext){
		logger.debug("#connected.cid:"+getChannelId());
	}
	
	public void onRead(ByteBuffer[] buffers,Object userContext){
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
	public void onFailure(Throwable t,Object userContext){
		onFailure(t);
	}
	public void onWriteFailure(Throwable t,Object[] userContexts){
		if(userContexts==null){
			onFailure(t,null);
			return;
		}
		onFailure(t,userContexts[0]);
	}
	public void onReadFailure(Throwable t,Object userContext){
		onFailure(t,userContext);
	}
	public void onAcceptFailure(Throwable t,Object userContext){
		onFailure(t,userContext);
	}
	public void onConnectFailure(Throwable t,Object userContext){
		onFailure(t,userContext);
	}
	public void onCloseFailure(Throwable t,Object userContext){
		onFailure(t,userContext);
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
		child.state=State.connect;
		return child;
	}
	
	public void finishChildHandler(){
		if(context==null){
			return;
		}
		context.finishChildContext();
	}
	
	/* for debug */
	public void dump(){
		dump(logger);
	}
	
	public void dump(Logger logger){
		logger.debug("$cid:"+getChannelId() + ":poolId:"+getPoolId() +":className:" +getClass().getName());
	}
	
	public String toString(){
		return "ChannelHandler[cid:"+getChannelId()+"]"+super.toString();
	}
}
