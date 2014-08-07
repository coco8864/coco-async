package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class ReadChannel implements BufferGetter,ChannelIO{
	private static Logger logger=Logger.getLogger(ReadChannel.class);
	private static long bufferMinLimit=8192;
	public enum State {
		init,
		selecting,
		reading,
		close
	}
	private State state;
	private ChannelContext context;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long totalReadLength;
	private long currentBufferLength;
	
	ReadChannel(ChannelContext context){
		this.context=context;
	}

	public void setup(){
		state=State.init;
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		store=Store.open(false);//storeはここでしか設定しない
		store.asyncBuffer(this, store);
		totalReadLength=currentBufferLength=0L;
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		long length=BuffersUtil.remaining(buffers);
		synchronized(context){
			currentBufferLength+=length;
			for(ByteBuffer buffer:buffers){
				workBuffer.add(buffer);
			}
			if(context.getContextOrders().doneRead(workBuffer)){
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

	public void doIo() {
		synchronized(context){
		}
		synchronized(context){
		}
	}

	void queueSelect(){
		state=State.selecting;
		context.getSelector().queueSelect(context);
	}
	
	boolean asyncConnect(){
		synchronized(context){
			queueSelect();
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
		context.getContextOrders().queueCallback(order);
		return true;
	}
	
	void readable(){
		synchronized(context){
			state=State.reading;
			IOManager.enqueue(this);
		}
	}
	
	void connectable(){
		synchronized(context){
			state=State.reading;
			IOManager.enqueue(this);
		}
	}
}
