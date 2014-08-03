package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class WriteChannel implements BufferGetter,ChannelIO{
	private static Logger logger=Logger.getLogger(WriteChannel.class);
	private static long bufferMinLimit=8192;
	public enum State {
		block,
		writable,
		io_queue,
		writing,
		close
	}
	private State state=State.writable;
	private ChannelContext context;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long totalWriteLength;
	private long currentBufferLength;

	public void setup(ChannelContext context){
		state=State.writable;
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		this.context=context;
		store=Store.open(false);//storeはここでしか設定しない
		store.asyncBuffer(this, store);
		totalWriteLength=currentBufferLength=0L;
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		long length=BuffersUtil.remaining(buffers);
		synchronized(context){
			currentBufferLength+=length;
			for(ByteBuffer buffer:buffers){
				workBuffer.add(buffer);
			}
			if(state==State.writable){
				state=State.io_queue;
				IOManager.enqueue(this);
			}else if(state==State.close){
				//TODO
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

	boolean asyncWrite(ByteBuffer[] buffers){
		store.putBuffer(buffers);
		return false;
	}
	
	boolean asyncClose(){
		return false;
	}
	
	void writable(){
		synchronized(context){
			if(state!=State.block){
				return;
			}else if(state==xx){
			}
			if(currentBufferLength==0){
				state=State.writable;
			}else{
				state=State.io_queue;
				IOManager.enqueue(this);
			}
		}
	}
	
	boolean isBlock(){
		synchronized(context){
			return (state==State.block);
		}
	}
}
