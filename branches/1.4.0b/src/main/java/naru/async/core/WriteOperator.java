package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.ChannelStastics;
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
		accepting,
		closing,
		acceptClosing,
		close
	}
	private State state;
	private SelectableChannel channel;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long totalWriteLength;
	private long currentBufferLength;
	//private boolean isAsyncClose;
	
	private ChannelContext context;
	private ChannelStastics stastics;
	private SelectOperator selectOperator;
	private OrderOperator orderOperator;
	
	WriteOperator(ChannelContext context){
		this.context=context;
	}

	void close(){
		state=State.close;
	}
	
	public void setup(SelectableChannel channel,boolean isServer){
		if(channel==null){
			close();
			return;
		}
		totalWriteLength=currentBufferLength=0L;
		this.channel=channel;
		this.stastics=context.getChannelStastics();
		this.selectOperator=context.getSelectOperator();
		this.orderOperator=context.getOrderOperator();
		if(isServer){
			state=State.accepting;
			store=null;
		}else{
			state=State.writable;
			store=Store.open(false);//storeはここでしか設定しない
			store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
			context.ref();//storeが生きている間contextを確保する
			store.asyncBuffer(this, store);
		}
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		logger.debug("onBuffer.cid:"+context.getPoolId()+":state:"+state);
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
		PoolManager.poolArrayInstance(buffers);
		return false;
	}

	public void onBufferEnd(Object userContext) {
		logger.debug("onBufferEnd.cid:"+context.getPoolId());
		PoolManager.poolBufferInstance(workBuffer);
		workBuffer.clear();
		store.unref();
		store=null;
		context.unref();
	}

	public void onBufferFailure(Object userContext, Throwable failure) {
		logger.debug("onBufferFailure",failure);
		onBufferEnd(userContext);
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
	
	private boolean afterWrite(long prepareBuffersLength,long writeLength,Throwable failure,boolean isClosing,boolean isAcceptClosing){
		if(failure!=null){
			orderOperator.failure(failure);
			closed();
			return false;
		}
		if(writeLength>0){
			currentBufferLength-=writeLength;
			totalWriteLength+=writeLength;
			orderOperator.doneWrite(totalWriteLength);
		}
		if(isClosing){
			orderOperator.doneClose(true);
			closed();
			return false;
		}
		if(isAcceptClosing){
			selectOperator.close();
			orderOperator.doneClose(false);
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
		logger.debug("doIo.cid:"+context.getPoolId()+":state:"+state);
		ByteBuffer[] prepareBuffers=null;
		synchronized(context){
			prepareBuffers=BuffersUtil.toByteBufferArray(workBuffer);
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
		}else{
			logger.debug("doIo prepareBuffersLength==0.cid:"+context.getPoolId());
		}
		PoolManager.poolArrayInstance(prepareBuffers);
		
		boolean isClosing=false;
		boolean isAcceptClosing=false;
		synchronized(context){
			isClosing=(state==State.closing);
			isAcceptClosing=(state==State.acceptClosing);
			if(isClosing){
				context.shutdownOutputSocket();
			}
			if(isAcceptClosing){
				context.closeSocket();
			}
			afterWrite(prepareBuffersLength, writeLength, failure, isClosing,isAcceptClosing);
		}
	}
	public ChannelContext getContext() {
		return context;
	}

	boolean asyncWrite(ByteBuffer[] buffers){
		switch(state){
		case block:
			break;
		case writable:
//			state=State.writing;
//			IOManager.enqueue(this);
		case writing:
		case closing:
			break;
		case close:
		case init:
		case accepting:
		case acceptClosing:
			logger.error("asyncWrite but state:"+state);
			PoolManager.poolBufferInstance(buffers);
			return false;
		}
		store.putBuffer(buffers);
		return true;
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
			if(store==null){
				return;
			}
			store.close();
		}
	}
	
	/* 0長受信した場合 */
	boolean onReadEos(){
		logger.debug("onReadEos.cid:"+context.getPoolId()+":"+state);
		return asyncClose();
	}
	
	boolean asyncClose(){
		switch(state){
		case block:
		case writable:
			IOManager.enqueue(this);
		case writing:
			state=State.closing;
			break;
		case accepting:
			IOManager.enqueue(this);
			state=State.acceptClosing;
			break;
		case init:
		case close:
		case closing:
		case acceptClosing:
			logger.debug("fail to asyncClose.cid:"+context.getPoolId()+":"+state);
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
