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
		close
	}
	private State state;
	private SelectableChannel channel;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long currentBufferLength;
	private long totalReadLength;
	
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

	public void setup(SelectableChannel channel){
		state=State.init;
		store=Store.open(false);//storeはここでしか設定しない
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		store.asyncBuffer(this, store);
		totalReadLength=currentBufferLength=0L;
		this.channel=channel;
		this.stastics=context.getChannelStastics();
		this.writeOperator=context.getWriteOperator();
		this.orderOperator=context.getOrderOperator();
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		long length=BuffersUtil.remaining(buffers);
		synchronized(context){
			currentBufferLength+=length;
			for(ByteBuffer buffer:buffers){
				workBuffer.add(buffer);
			}
			if(orderOperator.doneRead(workBuffer)){
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
		logger.debug("closed.cid:"+context.getPoolId());
		synchronized(context){
			if(isClose()){
				return;
			}
			state=State.close;
			orderOperator.checkAndCallbackFinish();
			context.unref();
			store.close();
			store.unref();
			store=null;
		}
	}
	
	private boolean executeRead() {
		ByteBuffer buffer=PoolManager.getBufferInstance();
		long length=0;
		boolean isEos=false;
		Throwable failure=null;
		try {
			//http://docs.oracle.com/javase/jp/6/api/java/nio/channels/ReadableByteChannel.html#read(java.nio.ByteBuffer)
			//EOS(End Of Stream)=-1 0はEOSじゃない
			length=((ReadableByteChannel)channel).read(buffer);
			logger.debug("##executeRead length:"+length +":cid:"+context.getPoolId());
			if(length>0){
				buffer.flip();
				totalReadLength+=length;
			}else if(length==0){
				PoolManager.poolBufferInstance(buffer);
			}else{
				PoolManager.poolBufferInstance(buffer);
				isEos=true;
			}
		} catch (IOException e) {
			PoolManager.poolBufferInstance(buffer);
			context.closeSocket();
			failure=e;
			context.dump();
			logger.warn("fail to read.cid:"+context.getPoolId() +":channel:"+ channel,failure);
		}
		synchronized(context){
			if(failure!=null){
				orderOperator.failure(failure);
				closed();
				return false;
			}else if(isEos){
				orderOperator.doneClose(false);
				writeOperator.onReadEos();
				closed();
			}else{
				store.putBuffer(BuffersUtil.toByteBufferArray(buffer));
				queueSelect(State.selectReading);
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
				orderOperator.failure(failure);
			}else{
				orderOperator.doneConnect();
				queueSelect(State.selectReading);
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

	public ChannelContext getContext() {
		return context;
	}

	void queueSelect(State state){
		if(this.state==State.close){
			return;
		}
		logger.debug("queueSelect.cid:"+context.getPoolId()+":"+this.state+">" +state);
		this.state=state;
		context.getSelector().queueSelect(context);
	}
	
	void queueIo(State state){
		if(this.state==State.close){
			return;
		}
		logger.debug("queueIo.cid:"+context.getPoolId()+":"+this.state+">" +state);
		this.state=state;
		IOManager.enqueue(this);
	}
	
	boolean asyncConnect(){
		synchronized(context){
			queueSelect(State.selectConnecting);
		}
		return true;
	}
	
	void asyncRead(Order order){
		if(currentBufferLength==0){
			context.getSelector().wakeup();
			return;
		}
		ByteBuffer[] bufs=BuffersUtil.toByteBufferArray(workBuffer);
		workBuffer.clear();
		currentBufferLength=0;
		order.setBuffers(bufs);
		orderOperator.queueCallback(order);
	}
	
	void readable(){
		logger.debug("readable.cid:"+context.getPoolId());
		synchronized(context){
			queueIo(State.reading);
		}
	}
	
	void connectable(){
		logger.debug("connectable.cid:"+context.getPoolId());
		synchronized(context){
			queueIo(State.connecting);
		}
	}
	
	boolean isClose(){
		return (state==State.close);
	}
}
