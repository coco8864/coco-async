package naru.async.store2;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import naru.async.BufferGetter;
import naru.async.Log;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.async.store.StoreCallback;
import naru.async.store.StoreManager;

public class Store extends PoolBase {
	class PageInfo{
		private int id;
		private int length;
	}
	private long sid;
	private String digest;
	private LinkedList<PageInfo> pages=new LinkedList<PageInfo>();
	private LinkedList<ByteBuffer> currentBuffers=new LinkedList<ByteBuffer>();
	private int currentLength;
	
	private BufferGetter bufferGetter;
	private Object userContext;
	private boolean canCallback;//callback���Ă悢���ۂ�
	private long borderLength;
	
	@Override
	public void recycle() {
		borderLength=PoolManager.getDefaultBufferSize()/2;
		this.canCallback=false;
	}
	
	public void asyncBuffer(BufferGetter bufferGetter,Object userContext){
		this.bufferGetter=bufferGetter;
		this.userContext=userContext;
		this.canCallback=true;
	}
	
	/* ���̏�����IO�����s���Ă͂����Ȃ�
	 * ���̏�������callback�����\���͂���
	 */
	public void putBuffer(ByteBuffer[] buffers){
		if(borderLength<=0||currentLength<borderLength){
			for(ByteBuffer buffer:buffers){
				currentLength+=buffer.remaining();
				currentBuffers.add(buffer);
			}
		}
		//currentBufferrs��buffers���Pbuffer�ɂ܂Ƃ߂�
		//PagePool.pushPage(buffer);
		//
	}
	
	public void nextBuffer(){
	}
	
	public void endBuffer(){
	}
	
	public void onPageIn(int pageId,ByteBuffer[] buffers){
	}
	
	/**
	 * callback�Ǘ����\�b�h�S
	 */
	//queue���ꂽ���Ԃ�callback����
	//callback���͏d�˂�callback���Ȃ�
	private LinkedList<StoreCallback> callbackQueue=new LinkedList<StoreCallback>();
	private boolean isCallbackProcessing=false;
	void callbackQueue(StoreCallback storeCallback){
		Log.debug(logger,"callbackQueue sid:",getStoreId());
		synchronized(callbackQueue){
			callbackQueue.addLast(storeCallback);
		}
		//sStoreManager.asyncDispatch(this);//TODO
	}
	
	void callback(){
		Log.debug(logger,"callback sid:",getStoreId());
		StoreCallback storeCallback=null;
		synchronized(callbackQueue){
			if(isCallbackProcessing || callbackQueue.size()<=0){
				Log.debug(logger,"callback loopout sid:",getPoolId());
				return;
			}
			isCallbackProcessing=true;
			storeCallback=callbackQueue.removeFirst();
		}
		while(true){
			storeCallback.callback();
			storeCallback.unref(true);
			synchronized(callbackQueue){
				if(callbackQueue.size()<=0){
					isCallbackProcessing=false;
					break;
				}
				storeCallback=callbackQueue.removeFirst();
			}
		}
	}
}
