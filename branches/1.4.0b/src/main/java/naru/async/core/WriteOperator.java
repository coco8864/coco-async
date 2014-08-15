package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class WriteOperator implements BufferGetter,ChannelIO{
	private static Logger logger=Logger.getLogger(WriteOperator.class);
	private static long bufferMinLimit=8192;
	public enum State {
		init,
		block,
		writable,
		writing,
		closing,
		close
	}
	private State state;
	private ChannelContext context;
	private SelectableChannel channel;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long totalWriteLength;
	private long currentBufferLength;
	private boolean isAsyncClose=false;
	
	WriteOperator(ChannelContext context){
		this.context=context;
	}

	public void setup(SelectableChannel channel){
		state=State.writable;
		store=Store.open(false);//storeはここでしか設定しない
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		store.asyncBuffer(this, store);
		totalWriteLength=currentBufferLength=0L;
		this.channel=channel;
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
	
	private long executeWrite(ByteBuffer[] prepareBuffers) throws IOException {
		Throwable failure=null;
		GatheringByteChannel channel=(GatheringByteChannel)this.channel;
		long length=channel.write(prepareBuffers);
		logger.debug("##executeWrite length:"+length +":cid:"+context.getPoolId());
		//lengthが0の事があるらしい,buffersの長さが0に違いない,buffer再利用の問題か？
		if(logger.isDebugEnabled()){
			logger.debug("executeWrite."+length +":cid:"+context.getPoolId()+":channel:"+channel);
		}
		return length;
	}
	
	private boolean afterWrite(long prepareBuffersLength,long writeLength,Throwable failure,boolean isClosing){
		if(failure!=null){
			context.getOrderOperator().failure(failure);
			closed();
			return false;
		}
		if(writeLength>0){
			currentBufferLength-=writeLength;
			totalWriteLength+=writeLength;
			context.getOrderOperator().doneWrite(totalWriteLength);
		}
		if(isClosing){
			context.getOrderOperator().doneClose(true);
			closed();
			return false;
		}
		if(prepareBuffersLength==writeLength){
			state=State.writable;
		}else{
			state=State.block;
		}
		Iterator<ByteBuffer> itr=workBuffer.iterator();
		while(itr.hasNext()){
			ByteBuffer buffer=itr.next();
			if( buffer.hasRemaining() ){
				break;
			}
			itr.remove();
			PoolManager.poolBufferInstance(buffer);
		}
		if(currentBufferLength<bufferMinLimit){
			store.asyncBuffer(this, store);
		}
		return true;
	}

	public long getTotalWriteLength() {
		return totalWriteLength;
	}

	public void doIo() {
		ByteBuffer[] prepareBuffers=null;
		boolean isClosing=false;
		synchronized(context){
			prepareBuffers=BuffersUtil.toByteBufferArray(workBuffer);
			isClosing=(state==State.closing);
		}
		
		long prepareBuffersLength=BuffersUtil.remaining(prepareBuffers);
		long writeLength=0;
		Throwable failure=null;
		if(prepareBuffersLength>0){
			try {
				writeLength=executeWrite(prepareBuffers);
			} catch (IOException e) {
				logger.warn("fail to write.channel:"+channel+":cid:"+context.getPoolId(),e);
				failure=e;
				context.closeSocket();
			}
		}
		if(isClosing){
			context.shutdownOutputSocket();
		}
		synchronized(context){
			afterWrite(prepareBuffersLength, writeLength, failure, isClosing);
		}
	}
	public void ref() {
		context.ref();
	}

	public void unref() {
		context.unref();
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
		case closing:
			PoolManager.poolBufferInstance(buffers);
			return false;
		}
		store.putBuffer(buffers);
		return true;
	}
	
	private boolean isRead0length=false;
	void read0length(){
		if(isRead0length){
			return;
		}
		isRead0length=true;
	}
	
	/* statusにcloseを設定する場合に呼び出す */
	private void closed(){
		logger.debug("closed.cid:"+context.getPoolId());
		synchronized(context){
			state=State.close;
			context.getOrderOperator().checkAndCallbackFinish();
			store.close();
		}
	}
	
	/* 0長受信した場合 */
	boolean onRead0(){
		return asyncClose();
	}
	
	boolean asyncClose(){
		if(isAsyncClose){
			return false;
		}
		isAsyncClose=true;
		switch(state){
		case block:
		case writable:
			IOManager.enqueue(this);
		case writing:
			state=State.closing;
			break;
		case init:
		case close:
		case closing:
			return false;
		}
		return true;
	}
	
	void writable(){
		logger.debug("writable.cid:"+context.getPoolId());
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
		return (state==State.block);
	}
	boolean isClose(){
		return (state==State.close);
	}
}
