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
		init,
		block,
		writable,
		writing,
		close
	}
	private State state;
	private ChannelContext context;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long totalWriteLength;
	private long currentBufferLength;
	private boolean isAsyncClose=false;
	
	WriteChannel(ChannelContext context){
		this.context=context;
	}

	public void setup(){
		state=State.init;
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
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
				state=State.writing;
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
		switch(state){
		case block:
			break;
		case writable:
			state=State.writing;
			IOManager.enqueue(this);
		case writing:
			break;
		case init:
		case close:
			return false;
		}
		store.putBuffer(buffers);
		return true;
	}
	
	boolean asyncClose(){
		if(isAsyncClose){
			return false;
		}
		switch(state){
		case block:
		case writable:
			state=State.writing;
			isAsyncClose=true;
			IOManager.enqueue(this);
		case writing:
			break;
		case init:
		case close:
			return false;
		}
		return true;
	}
	
	void writable(){
		synchronized(context){
			if(state==State.block){
				state=State.writable;
			}
			if(state==State.writable&&currentBufferLength!=0){
				state=State.writing;
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
