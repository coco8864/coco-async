package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class SelectOperator implements BufferGetter,ChannelIO{
	private static Logger logger=Logger.getLogger(SelectOperator.class);
	private static long bufferMinLimit=8192;
	enum State {
		init,
		accepting,
		connecting,
		reading,
		closing,
		close
	}
	private State state;
	private ChannelContext context;
	private SelectableChannel channel;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long currentBufferLength;
	private long totalReadLength;
	
	State getState(){
		return state;
	}
	
	SelectOperator(ChannelContext context){
		this.context=context;
	}

	public void setup(SelectableChannel channel){
		state=State.init;
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		store=Store.open(false);//storeはここでしか設定しない
		store.asyncBuffer(this, store);
		totalReadLength=currentBufferLength=0L;
		this.channel=channel;
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		long length=BuffersUtil.remaining(buffers);
		synchronized(context){
			currentBufferLength+=length;
			for(ByteBuffer buffer:buffers){
				workBuffer.add(buffer);
			}
			if(context.getOrderOperator().doneRead(workBuffer)){
				workBuffer.clear();//成功した場合workBufferはクリアされるが念のため
				currentBufferLength=0;
			}
			if(currentBufferLength<bufferMinLimit){
				store.asyncBuffer(this, store);
			}
		}
		return false;
	}

	public void onBufferEnd(Object userContext) {
	}

	public void onBufferFailure(Object userContext, Throwable failure) {
	}

	/* statusにcloseを設定する場合に呼び出す */
	private void closed(){
		synchronized(context){
			state=State.close;
			context.getOrderOperator().checkAndCallbackFinish();
		}
	}
	
	private boolean executeRead() {
		ByteBuffer buffer=PoolManager.getBufferInstance();
		long length=0;
		Throwable failure=null;
		try {
			length=((ReadableByteChannel)channel).read(buffer);
			buffer.flip();
			totalReadLength+=length;
			logger.debug("##executeRead length:"+length +":cid:"+context.getPoolId());
		} catch (IOException e) {
			failure=e;
			logger.warn("fail to read.cid:"+context.getPoolId() +":channel:"+ channel,failure);
		}
		if(failure!=null){
			PoolManager.poolBufferInstance(buffer);
			context.closeSocket();
			synchronized(context){
				context.getOrderOperator().failure(failure);
				closed();
			}
			context.dump();
			return false;
		}
		if(length>0){
			synchronized(context){
				store.putBuffer(BuffersUtil.toByteBufferArray(buffer));
				queueSelect(State.reading);
			}
			return true;
		}else{//0長受信
			PoolManager.poolBufferInstance(buffer);
			synchronized(context){
				context.getWriteOperator().onRead0();
				closed();
			}
		}
		return true;
	}
	
	public long getTotalReadLength() {
		return totalReadLength;
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
				context.getOrderOperator().failure(failure);
			}else{
				context.getOrderOperator().doneConnect();
				queueSelect(State.reading);
			}
		}
	}
	
	private void forceClose(){
		context.closeSocket();
		closed();
	}
	
	public void doIo() {
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
			executeRead();
		}
	}
	public void ref() {
		context.ref();
	}

	public void unref() {
		context.unref();
	}

	void queueSelect(State state){
		this.state=state;
		context.getSelector().queueSelect(context);
	}
	
	void queueIo(){
		queueIo(this.state);
	}
	
	void queueIo(State state){
		this.state=state;
		IOManager.enqueue(this);
	}
	
	boolean asyncConnect(){
		synchronized(context){
			queueSelect(State.connecting);
		}
		return true;
	}
	
	boolean asyncRead(Order order){
		if(currentBufferLength==0){
			return false;
		}
		ByteBuffer[] bufs=BuffersUtil.toByteBufferArray(workBuffer);
		workBuffer.clear();
		currentBufferLength=0;
		order.setBuffers(bufs);
		context.getOrderOperator().queueCallback(order);
		return true;
	}
	
	void readable(){
		synchronized(context){
			queueIo();
		}
	}
	
	void connectable(){
		synchronized(context){
			queueIo();
		}
	}
	
	boolean isClose(){
		return (state==State.close);
	}
}
