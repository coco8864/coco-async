package naru.async.store4;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.Log;
import naru.async.pool.BuffersUtil;
import naru.async.pool.Context;

public abstract class Store extends Context{
	private static Logger logger=Logger.getLogger(Store.class);
	protected static PageManager pageManager=PageManager.getInstance();
	protected static long borderLength=8192;
	
	protected enum State {
		OPEN,
		CALLBACKABLE,
		BLOCK,
		CLOSE
	}
	private BufferGetter bufferGetter;
	private Object userContext;
	private int storeId;
	private long callbackLength;
	
	protected State state;
	protected LinkedList<Integer> pages=new LinkedList<Integer>();
	
	@Override
	public void recycle() {
		this.callbackLength=0;
		this.state=State.CLOSE;
	}
	
	public void init(){
		init(null,null);
	}
	
	public void init(BufferGetter bufferGetter,Object userContext){
		this.bufferGetter=bufferGetter;
		this.userContext=userContext;
	}
	
	public abstract boolean putBuffer(ByteBuffer[] buffers);
	public abstract boolean nextBuffer();
	public abstract boolean closeBuffer();
	
	/**
	 * callback管理メソッド郡
	 */
	//queueされた順番にcallbackする
	//callback中は重ねてcallbackしない
	/* callback制御 */
	private enum CallbackType {
		BUFFER,
		END,
		FAILURE,
	}
	private LinkedList<CallbackType> callbackQueue=new LinkedList<CallbackType>();
	private void callbackQueue(CallbackType callbackType){
		synchronized(callbackQueue){
			callbackQueue.addLast(callbackType);
		}
	}
	private ByteBuffer[] cbBuffers;
	private Throwable cbFailure;
	private boolean isCallbackProcessing=false;
	
	protected void queueBufferCallback(List<ByteBuffer>cbBuffers){
		if(this.cbBuffers!=null){
			logger.error("duplicate cbBuffers");
		}
		this.cbBuffers=BuffersUtil.toByteBufferArray(cbBuffers);
		callbackLength+=BuffersUtil.remaining(this.cbBuffers);
		callbackQueue(CallbackType.BUFFER);
	}
	
	protected void queueFailureCallback(Throwable cbFailure){
		if(this.cbFailure!=null){
			logger.error("duplicate cbFailure");
		}
		this.cbFailure=cbFailure;
		callbackQueue(CallbackType.FAILURE);
	}
	
	protected void queueEndCallback(){
		callbackQueue(CallbackType.END);
	}
	
	protected boolean callback(){
		Log.debug(logger,"callback storeId:",storeId);
		CallbackType type;
		synchronized(callbackQueue){
			if(isCallbackProcessing || callbackQueue.size()<=0){
				Log.debug(logger,"callback loopout sid:",getPoolId());
				return false;
			}
			isCallbackProcessing=true;
			type=callbackQueue.removeFirst();
		}
		while(true){
			switch(type){
			case BUFFER:
				boolean next=false;
				//callbackLength+=BuffersUtil.remaining(cbBuffers);
				try{
					next=bufferGetter.onBuffer(cbBuffers, userContext);
				}catch(Throwable t){
					logger.error("bufferGetter.onBuffer error",t);
				}
				cbBuffers=null;
				if(next){
					synchronized(this){
						state=State.CALLBACKABLE;
					}
				}
				break;
			case END:
				try{
					bufferGetter.onBufferEnd(userContext);
				}catch(Throwable t){
					logger.error("bufferGetter.onBufferEnd error",t);
				}
				break;
			case FAILURE:
				try{
					bufferGetter.onBufferFailure(cbFailure,userContext);
				}catch(Throwable t){
					logger.error("bufferGetter.onBufferFailure error",t);
				}
				cbFailure=null;
				break;
			}
			synchronized(callbackQueue){
				if(callbackQueue.size()<=0){
					isCallbackProcessing=false;
					break;
				}
				type=callbackQueue.removeFirst();
			}
		}
		return true;
	}
}
