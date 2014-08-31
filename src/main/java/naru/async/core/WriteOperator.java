package naru.async.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
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
		prepare,
		block,
		writable,
		close
	}
	private State state;
	private boolean isAsyncClose;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private long totalWriteLength;
	private long currentBufferLength;
	
	/* 1)buf:current/notAll/all
	 * 2)state:prepare/block/write/close
	 * 3)isAsyncClose:true/false
	 * 
	 * a)current:block:true|false -> wait
	 * b)current:writable:true|false -> io
	 * c)notAll:writable:true|false -> wait
	 * d)all:writable:false -> wait
	 * e)all:writable:true -> io
	 * 
	 * f)all:prepare:false ->wait
	 * g)all:prepare:true ->io
	 * h)*:close:* ->wait
	 */
	
	private boolean isIo(){
		if(state==State.close||state==State.prepare){
			return false;
		}
		if(currentBufferLength!=0){
			if(state==State.block){
				return false;
			}
			if(state==State.writable){
				return true;
			}
		}else if(totalWriteLength!=store.getPutLength()){
			return false;
		}else{
			if(isAsyncClose){
				return true;
			}else{
				return false;
			}
		}
		logger.warn("sate:"+state+":isAsyncClose:"+isAsyncClose+":currentBufferLength:"+currentBufferLength);
		return false;
	}
	
	private SelectableChannel channel;
	
	private ChannelContext context;
	private ChannelStastics stastics;
	private SelectOperator selectOperator;
	private OrderOperator orderOperator;
	
	WriteOperator(ChannelContext context){
		this.context=context;
	}

	public void setup(SelectableChannel channel){
		this.isAsyncClose=false;
		totalWriteLength=currentBufferLength=0L;
		if(channel==null){//in case SPDY
			state=State.close;
			return;
		}
		this.channel=channel;
		this.stastics=context.getChannelStastics();
		this.selectOperator=context.getSelectOperator();
		this.orderOperator=context.getOrderOperator();
		if(channel instanceof SocketChannel){
			SocketChannel socketChannel=(SocketChannel)channel;
			if(socketChannel.isConnected()){
				state=State.writable;
			}else{
				state=State.prepare;
			}
			store=Store.open(false);//storeはここでしか設定しない
			store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
			context.ref();//storeが生きている間contextを確保する
			store.asyncBuffer(this, store);
		}else{
			state=State.prepare;
			store=null;
		}
	}
	
	public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
		logger.debug("onBuffer.cid:"+context.getPoolId()+":state:"+state);
		long length=BuffersUtil.remaining(buffers);
		synchronized(context){
			for(ByteBuffer buffer:buffers){
				workBuffer.add(buffer);
			}
			if(currentBufferLength==0){
				if(isIo()==false){
					IOManager.enqueue(this);
				}
			}
			currentBufferLength+=length;
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
		currentBufferLength=0;
		store.unref();
		store=null;
		context.unref();
	}

	public void onBufferFailure(Throwable failure, Object userContext) {
		logger.debug("onBufferFailure",failure);
		onBufferEnd(userContext);
	}
	
	private long executeWrite(ByteBuffer[] prepareBuffers) throws IOException {
		GatheringByteChannel channel=(GatheringByteChannel)this.channel;
		long length=channel.write(prepareBuffers);
		logger.debug("##executeWrite length:"+length +":cid:"+context.getPoolId());
		if(logger.isDebugEnabled()){
			logger.debug("executeWrite."+length +":cid:"+context.getPoolId()+":channel:"+channel);
		}
		return length;
	}
	
	private boolean afterWrite(long prepareBuffersLength,long writeLength,Throwable failure,boolean isHalfClose,boolean isAllClose){
		if(failure!=null){
			orderOperator.failure(failure);
			closed();
			return false;
		}
		if(isHalfClose){
			orderOperator.doneClose(true);
			closed();
			return false;
		}
		if(isAllClose){
			selectOperator.close();
			orderOperator.doneClose(false);
			closed();
			return false;
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
		if(isClose()){
			return false;
		}
		if(currentBufferLength==0){
			state=State.writable;
			return false;
		}else if(prepareBuffersLength!=writeLength){
			state=State.block;
			return false;
		}
		return true;
	}

	public long getTotalWriteLength() {
		return totalWriteLength;
	}
	
	private boolean completeIo(long prepareBuffersLength,long writeLength,Throwable failure){
		boolean isHalfClose=false;
		boolean isAllClose=false;
		synchronized(context){
			if(writeLength>0){
				currentBufferLength-=writeLength;
				totalWriteLength+=writeLength;
				orderOperator.doneWrite(totalWriteLength);
			}
			if(isAsyncClose&&(store.getPutBufferLength()==totalWriteLength)){
				context.shutdownOutputSocket();
				isHalfClose=true;
			}else if(!isAsyncClose&&state==State.close){
				context.closeSocket();
				isAllClose=true;
			}
			return afterWrite(prepareBuffersLength, writeLength, failure, isHalfClose,isAllClose);
		}
	}
	
	public void doIo() {
		logger.debug("doIo.cid:"+context.getPoolId()+":state:"+state+":currentBufferLength:"+currentBufferLength);
		while(true){
			if(isClose()){
				logger.warn("doIo when close");
				return;
			}
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
				logger.debug("doIo prepareBuffersLength==0.cid:"+context.getPoolId()+":state:"+state);
			}
			PoolManager.poolArrayInstance(prepareBuffers);
			if(completeIo(prepareBuffersLength, writeLength, failure)==false){
				break;
			}
		}
	}
	public ChannelContext getContext() {
		return context;
	}

	boolean asyncWrite(ByteBuffer[] buffers){
		if(isClose()){
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
		if(isAsyncClose){
			return false;
		}
		if(isClose()){
			return false;
		}
		boolean isIoBefore=isIo();
		isAsyncClose=true;
		boolean isIoAfter=isIo();
		if(!isIoBefore&&isIoAfter){
			IOManager.enqueue(this);
		}
		return true;
	}
	
	void writable(){
		logger.debug("writable.cid:"+context.getPoolId());
		if(isClose()){
			return;
		}
		boolean isIoBefore=isIo();
		state=State.writable;
		boolean isIoAfter=isIo();
		if(!isIoBefore&&isIoAfter){
			IOManager.enqueue(this);
		}
	}
	
	boolean isBlock(){
		return (state==State.block);
	}
	boolean isClose(){
		return (state==State.close);
	}
}
