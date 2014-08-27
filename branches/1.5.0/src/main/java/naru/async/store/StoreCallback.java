package naru.async.store;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;

public class StoreCallback extends PoolBase{
	private static Logger logger=Logger.getLogger(StoreCallback.class);
	/* callback制御 */
	private enum CallbackType {
		BUFFER,
		END,
		FAILURE,
	}
	private Store store;
	private CallbackType callbackType;
	private BufferGetter callbackBufferGetter;
	private Object callbackUserContext;
	private Throwable callbackFailure;
	private ByteBuffer[] callbackBuffer;

	@Override
	public void recycle() {
		setStore(null);
		callbackBuffer=null;
		super.recycle();
	}
	
	private void setStore(Store store){
		if(store!=null){
			store.ref();
		}
		if(this.store!=null){
			this.store.unref();
		}
		this.store=store;
	}
	
	public void asyncBuffer(Store store,BufferGetter bufferGetter,Object userContext,ByteBuffer[] buffer){
		setStore(store);
		callbackBufferGetter=bufferGetter;
		callbackUserContext=userContext;
		callbackBuffer=buffer;
		callbackType=CallbackType.BUFFER;
		store.callbackQueue(this);
//		StoreManager.asyncDispatch(this);
	}
	public void asyncBufferEnd(Store store,BufferGetter bufferGetter,Object userContext){
		setStore(store);
		callbackBufferGetter=bufferGetter;
		callbackUserContext=userContext;
		callbackType=CallbackType.END;
		store.callbackQueue(this);
//		StoreManager.asyncDispatch(this);
	}
	public void asyncBufferFailure(Store store,BufferGetter bufferGetter,Object userContext,Throwable failure){
		setStore(store);
		callbackBufferGetter=bufferGetter;
		callbackUserContext=userContext;
		callbackFailure=failure;
		callbackType=CallbackType.FAILURE;
		store.callbackQueue(this);
//		StoreManager.asyncDispatch(this);
	}
	
	/**
	 * callback中に各種リクエストが来た事を判定する
	 * callbackがリカーシブルするとまずいが、callbackは直接呼び出さず、dispatcher経由なのでない
	 * close()からの直接呼び出しはあるが...
	 */
//	private static ThreadLocal<Store> callbackStore=new ThreadLocal<Store>();
//	public static boolean isCallbackProcessing(Store store){
//		return (store==callbackStore.get());
//	}
	
	public void callback(){
//		store.beforeCallback();
		boolean isEnd=true;
		boolean onBufferReturn=false;
//		callbackStore.set(store);
		try{
			switch(callbackType){
			case BUFFER:
				isEnd=false;
				store.countCallbackBuffer(BuffersUtil.remaining(callbackBuffer));
				onBufferReturn=callbackBufferGetter.onBuffer(callbackBuffer, callbackUserContext);
				callbackBuffer=null;
				break;
			case END:
				store.countCallbackEnd();
				callbackBufferGetter.onBufferEnd(callbackUserContext);
				break;
			case FAILURE:
				store.countCallbackFailure();
				callbackBufferGetter.onBufferFailure(callbackFailure, callbackUserContext);
				break;
			}
		}catch(Throwable t){
			logger.error("StoreCallback#callback error",t);
		}
		store.doneCallback(isEnd,onBufferReturn,callbackBufferGetter,callbackUserContext);
	}

}
