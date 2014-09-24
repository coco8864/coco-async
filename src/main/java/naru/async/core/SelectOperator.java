package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.ChannelStastics;
import naru.async.Log;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class SelectOperator implements BufferGetter,ChannelIO{
	private static Logger logger=Logger.getLogger(SelectOperator.class);
	private static long bufferMinLimit=8192;
	enum State {
		init,
		accepting,
		selectConnecting,
		connecting,
		selectReading,
		reading,
		closing,
		closeSuspend,
		close
	}
	private State state;
	private SelectableChannel channel;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long currentBufferLength;
	private long totalCallbackLength;
	
	private ChannelContext context;
	private ChannelStastics stastics;
	private WriteOperator writeOperator;
	private OrderOperator orderOperator;
	
	SelectOperator(ChannelContext context){
		this.context=context;
	}
	
	State getState(){
		return state;
	}
	
	void close(){
		state=State.close;
	}

	public void setup(SelectableChannel channel){
		if(channel==null){//in case SPDY
			close();
			return;
		}
		state=State.init;
		totalCallbackLength=currentBufferLength=0L;
		this.channel=channel;
		this.stastics=context.getChannelStastics();
		this.writeOperator=context.getWriteOperator();
		this.orderOperator=context.getOrderOperator();
		if(channel instanceof SocketChannel){
			store=Store.open(false);//storeはここでしか設定しない
			store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
			context.ref();//storeが生きている間contextを確保する
			Log.debug(logger,"setup.cid:",context.getPoolId(),":sid:",store.getStoreId() );
			store.asyncBuffer(this, store);
		}else{
			store=null;
		}
	}
	
	public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
		Log.debug(logger, "onBuffer.cid:",context.getPoolId());
		long length=BuffersUtil.remaining(buffers);
		synchronized(context){
			currentBufferLength+=length;
			for(ByteBuffer buffer:buffers){
				workBuffer.add(buffer);
			}
			if(orderOperator.doneRead(workBuffer)){
				workBuffer.clear();//成功した場合workBufferはクリアされるが念のため
				totalCallbackLength+=currentBufferLength;
				currentBufferLength=0;
			}
			if(state==State.closeSuspend){
				orderOperator.doneClose(false);
				writeOperator.onReadEos();
				closed();
				Log.debug(logger,"closeSuspend->",state,":cid:",context.getPoolId(),":totalCallbackLength:",totalCallbackLength);
			}else if(currentBufferLength<bufferMinLimit){
				store.asyncBuffer(this, store);
			}
		}
		PoolManager.poolArrayInstance(buffers);
		return false;
	}

	public void onBufferEnd(Object userContext) {
		Log.debug(logger,"onBufferEnd.cid:",context.getPoolId());
		PoolManager.poolBufferInstance(workBuffer);
		workBuffer.clear();
		currentBufferLength=0;
		store.unref();
		store=null;
		context.unref();
	}

	public void onBufferFailure(Throwable failure, Object userContext) {
		Log.debug(logger,"onBufferFailure",failure);
		onBufferEnd(userContext);
	}

	/* statusにcloseを設定する場合に呼び出す */
	private void closed(){
		Log.debug(logger,"closed.cid:",context.getPoolId());
		synchronized(context){
			if(isClose()){
				return;
			}
			state=State.close;
			orderOperator.checkAndCallbackFinish();
			if(store!=null){
				store.close(this,null);
			}
		}
	}
	
	private boolean executeRead() {
		ByteBuffer buffer=PoolManager.getBufferInstance();
		long length=0;
		boolean isEos=false;
		Throwable failure=null;
		boolean readable=false;
		try {
			//http://docs.oracle.com/javase/jp/6/api/java/nio/channels/ReadableByteChannel.html#read(java.nio.ByteBuffer)
			//EOS(End Of Stream)=-1 0はEOSじゃない
			length=((ReadableByteChannel)channel).read(buffer);
			Log.debug(logger,"##executeRead length:",length,":cid:",context.getPoolId());
			if(length>0){
				buffer.flip();
				if(buffer.hasRemaining()==false){
					readable=true;//用意したバッファを全部つかちゃったって事はまだreadable
				}
			}else if(length==0){
				PoolManager.poolBufferInstance(buffer);
				buffer=null;
			}else{
				PoolManager.poolBufferInstance(buffer);
				buffer=null;
				isEos=true;
			}
		} catch (IOException e) {
			PoolManager.poolBufferInstance(buffer);
			failure=e;
			context.dump();
			logger.warn("fail to read.cid:"+context.getPoolId() +":channel:"+ channel,failure);
			context.closeSocket();
		}
		synchronized(context){
			if(failure!=null){
				orderOperator.failure(failure);
				writeOperator.onReadEos();
				closed();
				return false;
			}else if(isEos){
				if(orderOperator.isReadOrder()&&totalCallbackLength!=store.getPutBufferLength()){
					Log.debug(logger,"closeSuspend.cid:",context.getPoolId(),":totalCallbackLength:",totalCallbackLength,":store.getPutBufferLength():",store.getPutBufferLength());
					state=State.closeSuspend;
				}else{
					orderOperator.doneClose(false);
					writeOperator.onReadEos();
					closed();
				}
				return false;
			}else{
				if(buffer!=null){
					Log.debug(logger,"store.putBuffer.cid:",context.getPoolId());
					store.putBuffer(BuffersUtil.toByteBufferArray(buffer));
				}
				if(readable==false){
					queueSelect(State.selectReading);
					return false;
				}
			}
		}
		return true;
	}
	
	public long getTotalReadLength() {
		if(store==null){
			return 0;
		}
		return store.getPutBufferLength();
	}

	private void finishConnect(){
		Throwable failure=null;
		try {
			((SocketChannel)channel).finishConnect();//リモートが止まっている場合は、ここでConnectExceptionとなる。
		} catch (IOException e) {
			context.closeSocket();
			failure=e;
		}
		synchronized(context){
			if(failure!=null){
				orderOperator.failure(failure);
			}else{
				context.setupSocketOpt();
				orderOperator.doneConnect();
				queueSelect(State.selectReading);
			}
		}
	}
	
	private void forceClose(){
		context.closeSocket();
		synchronized(context){
			orderOperator.doneClose(false);
			closed();
		}
	}
	
	public void doIo() {
		Log.debug(logger,"doIo.cid:",context.getPoolId(),":state:",state);
		boolean isConnect;
		boolean isRead;
		boolean isClose;
		synchronized(context){
			isConnect=(state==State.connecting);
			isRead=(state==State.reading);
			isClose=(state==State.closing);
		}
		if(isClose){
			forceClose();
		}else if(isConnect){
			finishConnect();
		}else if(isRead){
			while(executeRead()){};
		}
	}

	public ChannelContext getContext() {
		return context;
	}

	void queueSelect(State state){
		if(this.state==State.close||this.state==State.selectReading){
			return;
		}
		Log.debug(logger,"queueSelect.cid:",context.getPoolId(),":",this.state,">",state);
		this.state=state;
		context.getSelector().queueSelect(context);
	}
	
	void queueIo(State state){
		if(this.state==State.close){
			return;
		}
		Log.debug(logger,"queueIo.cid:",context.getPoolId(),":",this.state,">",state);
		this.state=state;
		IOManager.enqueue(this);
	}
	
	boolean asyncConnect(){
		synchronized(context){
			queueSelect(State.selectConnecting);
		}
		return true;
	}
	
	boolean asyncRead(Order order){
		if(currentBufferLength==0){
			context.getSelector().wakeup();
			return false;
		}
		ByteBuffer[] bufs=BuffersUtil.toByteBufferArray(workBuffer);
		workBuffer.clear();
		totalCallbackLength+=currentBufferLength;
		currentBufferLength=0;
		store.asyncBuffer(this, store);
		order.setBuffers(bufs);
		orderOperator.queueCallback(order);
		return true;
	}
	
	void readable(){
		Log.debug(logger,"readable.cid:",context.getPoolId());
		queueIo(State.reading);
	}
	
	void connectable(){
		Log.debug(logger,"connectable.cid:",context.getPoolId());
		queueIo(State.connecting);
	}
	
	boolean isClose(){
		return (state==State.close);
	}
	boolean isAccepting(){
		return (state==State.accepting);
	}
}
