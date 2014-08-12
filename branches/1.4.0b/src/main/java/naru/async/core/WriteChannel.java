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

public class WriteChannel implements BufferGetter,ChannelIO{
	private static Logger logger=Logger.getLogger(WriteChannel.class);
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
	
	WriteChannel(ChannelContext context){
		this.context=context;
	}

	public void setup(SelectableChannel channel){
		state=State.init;
		store.ref();//store�������I����Ă����̃I�u�W�F�N�g�������Ă���ԕێ�����
		context.ref();//store�������Ă����context���m�ۂ���
		store=Store.open(false);//store�͂����ł����ݒ肵�Ȃ�
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
	
	private boolean executeClose() {
		context.shutdownOutputSocket();
		synchronized(context){
			state=State.close;
			context.getContextOrders().doneClose();
		}
	}
	
	private boolean executeWrite(ByteBuffer[] prepareBuffers) {
		Throwable failure=null;
		GatheringByteChannel channel=(GatheringByteChannel)this.channel;
		long length=0;
		long prepareBuffersLength=BuffersUtil.remaining(prepareBuffers);
		try {
			length=channel.write(prepareBuffers);
			logger.debug("##executeWrite length:"+length +":cid:"+context.getPoolId());
			//length��0�̎�������炵��,buffers�̒�����0�ɈႢ�Ȃ�,buffer�ė��p�̖�肩�H
			if(logger.isDebugEnabled()){
				logger.debug("executeWrite."+length +":cid:"+context.getPoolId()+":channel:"+channel);
			}
		} catch (IOException e) {
			logger.warn("fail to write.channel:"+channel+":cid:"+context.getPoolId(),e);
			failure=e;
			context.closeSocket();
		}
		synchronized(context){
			if(failure!=null){
				context.getContextOrders().failure(failure);
				state=State.close;
				return false;
			}else if(prepareBuffersLength==length){
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
			currentBufferLength-=length;
			totalWriteLength+=length;
			context.getContextOrders().doneWrite(totalWriteLength);
			if(currentBufferLength<bufferMinLimit){
				store.asyncBuffer(this, store);
			}
		}
		return true;
	}

	public void doIo() {
		synchronized(context){
			//write or close
			ByteBuffer[] prepareBuffers=BuffersUtil.toByteBufferArray(workBuffer);
		}
		synchronized(context){
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
