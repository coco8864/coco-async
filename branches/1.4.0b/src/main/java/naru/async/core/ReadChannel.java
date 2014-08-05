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

	public void setup(ChannelContext context){
		state=State.select_queue;
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		this.context=context;
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
			context.queueCallback(order);
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

	boolean asyncConnect(){
		return false;
	}
	boolean asyncRead(){
		return false;
	}
	
	void readable(){
		synchronized(context){
		}
	}
	
	void connectable(){
		synchronized(context){
		}
	}
}
