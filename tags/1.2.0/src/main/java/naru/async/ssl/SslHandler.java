package naru.async.ssl;

import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;
/**
 * SSLを考慮するための雛形
 * 以下、asyncRead呼び出しは、このクラスが自動的行う
 * 1)handshake中にネットワークデータが必要な場合
 * 2)データでコード時にBUFFER_UNDERFLOWとなった場合
 * 3)サーバモードで最初のユーザデータを要求するためのasyncRead(!!!重要!!!)
 * 4)サーバ、クライアントモードによらず、doHandshakeがtrue復帰し、かつネットワークデータが受信ずみでなかった場合のasyncRead(!!!重要!!!)
 * 
 * @author naru
 *
 */
public abstract class SslHandler extends ChannelHandler {
	private static Logger logger = Logger.getLogger(SslHandler.class);
	/* readのuserContextの引継ぎ用 */
	private Object closeLock=new Object();
	private SslAdapter sslAdapter;
	private Store readPeekStore;
	private Store writePeekStore;
	
	public void recycle() {
		if(sslAdapter!=null){
			sslAdapter.unref(true);
			sslAdapter=null;
		}
		this.readPeekStore=null;
		this.writePeekStore=null;
		super.recycle();
	}

	public abstract SSLEngine getSSLEngine();
	
//	public void setSslAdapter(SslAdapter sslAdapter){
//		this.sslAdapter=sslAdapter;
//	}
	
	public ChannelHandler forwardHandler(SslHandler handler){
		SslAdapter sslAdapter=this.sslAdapter;
		Store readPeekStore=this.readPeekStore;
		Store writePeekStore=this.writePeekStore;
		this.sslAdapter=null;
		this.readPeekStore=null;
		this.writePeekStore=null;
		super.forwardHandler(handler);
		handler.sslAdapter=sslAdapter;
		handler.readPeekStore=readPeekStore;
		handler.writePeekStore=writePeekStore;
		if(sslAdapter!=null){
//			sslAdapter.setHandler(handler);
			sslAdapter.forwardHandler(handler);
		}
		return handler;
	}

	public ChannelHandler forwardHandler(Class handlerClass){
		SslAdapter sslAdapter=this.sslAdapter;
		Store readPeekStore=this.readPeekStore;
		Store writePeekStore=this.writePeekStore;
		this.sslAdapter=null;
		this.readPeekStore=null;
		this.writePeekStore=null;
		
		long cid=getChannelId();
		SslHandler handler= (SslHandler)super.forwardHandler(handlerClass);
		if(handler==null){//既にcloseされている等でforwardに失敗
			logger.debug("forwardHandler ng.cid:"+cid);
			this.sslAdapter=sslAdapter;
			this.readPeekStore=readPeekStore;
			this.writePeekStore=writePeekStore;
			return null;
		}
		handler.sslAdapter=sslAdapter;
		handler.readPeekStore=readPeekStore;
		handler.writePeekStore=writePeekStore;
		if(sslAdapter!=null){
			logger.debug("forwardHandler ok.cid:"+cid);
//			sslAdapter.setHandler(handler);
			sslAdapter.forwardHandler(handler);
		}
		return handler;
	}
	
	/**
	 * 読み込んだ平文を処理するメソッド
	 * @param buffers
	 */
	public void onReadPlain(Object userContext,ByteBuffer[] buffers) {
		//オーバライドしてね
		return;
	}
	
	/**
	 * asyncWriteに対する完了通知メソッド
	 * onWrittenは、sslで分割された書き込みに対応するため１対１に対応しない
	 * @param buffers
	 */
	public void onWrittenPlain(Object userContext) {
		//オーバライドしてね
		return;
	}
	
	public final void onWritten(Object userContext){
		logger.debug("onWritten cid:" + getChannelId() + ":userContext:"+userContext +":sslAdapter:"+sslAdapter);
		if(sslAdapter==null){
			onWrittenPlain(userContext);
			return;
		}
		sslAdapter.onWritten(userContext);
	}
	
	/**
	 * SSLhandshakeが正常に終了した直後によびだされる、メソッド
	 * このメソッドの中でasyncReadを呼び出してはいけない
	 * その代わり、復帰値がtrueの場合には、必要に応じてasync側でasyncReadを呼び出す
	 * @return ネットワークかからの次のデータが必要な場合true
	 */
	public boolean onHandshaked() {
		//オーバライドしてね
		return false;
	}
	
	public boolean isSsl(){
		return (sslAdapter!=null);
	}
	
	public boolean sslOpen(boolean isClientMode){
		if(sslAdapter!=null){
			logger.error("sslOpen aleady set sslAdapter");
			sslAdapter.unref();
		}
		sslAdapter=(SslAdapter) PoolManager.getInstance(SslAdapter.class);
		return sslAdapter.open(isClientMode, this);
	}
	
	public boolean sslOpenWithBuffer(boolean isClientMode,ByteBuffer[] buffers){
		if(sslAdapter!=null){
			logger.error("sslOpenWithBuffer aleady set sslAdapter");
			sslAdapter.unref();
		}
		sslAdapter=(SslAdapter) PoolManager.getInstance(SslAdapter.class);
		return sslAdapter.openWithBuffer(isClientMode, this, buffers);
	}
	
	/**
	 * 例えばの実装
	 */
	public void onAccepted(Object userContext) {
//		sslOpen(false,readTimeout,writeTimeout);
	}
	
	public void callbackReadPlain(Object userContext, ByteBuffer[] buffers){
		if(readPeekStore!=null){
			ByteBuffer[] peekBuffers=PoolManager.duplicateBuffers(buffers);;
			readPeekStore.putBuffer(peekBuffers);
		}
		onReadPlain(userContext,buffers);
	}
	
	/**
	 * 必要がある場合、平文化する
	 */
	public void onRead(Object userContext, ByteBuffer[] buffers) {
		if(sslAdapter==null){
			callbackReadPlain(userContext,buffers);
			return;
		}
		if(userContext==SslAdapter.SSLCTX_PLAIN_DATA){
			callbackReadPlain(userContext,buffers);
			return;
		}else if(userContext==SslAdapter.SSLCTX_CLOSE_NETWORK){
			PoolManager.poolBufferInstance(buffers);//読み捨て
//			asyncClose(SslAdapter.SSLCTX_CLOSE_NETWORK);
			return;
		}
		try {
			/* この延長でもう一度onReadが呼び出される */
			sslAdapter.onRead(userContext, buffers);
		} catch (SSLException e) {
//			sslAdapter.print();
			onReadFailure(userContext, e);
			logger.error("read ssl error.cid:"+getChannelId(),e);
			//復号化失敗時の処理
		}
	}
	
	//SSLEngineにまだデータが残っているかもしれない。
	public void onClosed(Object userContext) {
		logger.debug("#closed.cid:" + getChannelId());
		if(sslAdapter!=null){
			/* この延長でonReadが呼び出されるかもしれない */
			sslAdapter.closeInbound();
		}
		super.onClosed(userContext);
	}

	public void onFinished() {
		logger.debug("#finished.cid:" + getChannelId() +":sslAdapter:" +sslAdapter);
		synchronized(closeLock){
			if(sslAdapter!=null){
				sslAdapter.unref(true);
				sslAdapter=null;
			}
		}
	}
	
	/**
	 * 必要がある場合、暗号化して送信するようにオーバーライド
	 */
	public boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
		if(sslAdapter==null){
			//sslContext配下にない場合は、回線に直接投げる
			if(writePeekStore!=null){
//				ByteBuffer[] peekBuffers=BuffersUtil.dupBuffers(buffers);
				ByteBuffer[] peekBuffers=PoolManager.duplicateBuffers(buffers);
				writePeekStore.putBuffer(peekBuffers);
			}
			logger.debug("asyncWrite1 cid:"+getChannelId());
			return super.asyncWrite(userContext, buffers);
		}
		if(userContext==SslAdapter.SSLCTX_WRITE_NETWORK){
			logger.debug("asyncWrite2 cid:"+getChannelId());
			//SslAdapterから呼び出された場合は、回線に直接投げる
			return super.asyncWrite(userContext, buffers);
		}
		
		if(writePeekStore!=null){
//			ByteBuffer[] peekBuffers=BuffersUtil.dupBuffers(buffers);
			ByteBuffer[] peekBuffers=PoolManager.duplicateBuffers(buffers);
			writePeekStore.putBuffer(peekBuffers);
		}
		try {
			return sslAdapter.asyncWrite(userContext, buffers);
		} catch (SSLException e) {
			logger.error("asyncWrite ssl error",e);
			asyncClose(null);
		}
		return true;
	}
	
	/**
	 * SSL処理を考慮してasyncCloseを発行するメソッド
	 */
	public boolean asyncClose(Object userContext){
		if(sslAdapter==null){
			return super.asyncClose(userContext);
		}
		if(userContext==SslAdapter.SSLCTX_CLOSE_NETWORK){
			return super.asyncClose(userContext);
		}
		return sslAdapter.asyncClose(userContext);
	}
	
	/*----以降プロトコルpeek関連----*/
	public void pushReadPeekStore(Store readPeekStore){
		this.readPeekStore=readPeekStore;
	}
	public void pushWritePeekStore(Store writePeekStore){
		this.writePeekStore=writePeekStore;
	}
	
	public Store popReadPeekStore(){
		Store readPeekStore=this.readPeekStore;
		this.readPeekStore=null;
		return readPeekStore;
	}
	public Store popWritePeekStore(){
		Store writePeekStore=this.writePeekStore;
		this.writePeekStore=null;
		return writePeekStore;
	}
}
