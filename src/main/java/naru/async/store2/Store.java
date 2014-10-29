package naru.async.store2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.Log;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public class Store extends PoolBase {
	private static Logger logger=Logger.getLogger(Store.class);
	private static PagePool pagePool=PagePool.getInstance();
	/*class PageInfo{
		private int id;
		private int length;
	}
	*/
	private long sid;
	private String digest;
	private LinkedList<Integer> pages=new LinkedList<Integer>();
	private LinkedList<ByteBuffer> currentBuffers=new LinkedList<ByteBuffer>();
	private long currentLength;
	private long putLength;
	private long callbackLength;
	
	private BufferGetter bufferGetter;
	private Object userContext;
	private boolean canCallback;//callbackしてよいか否か
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
	
	private synchronized void putBufferInternal(ByteBuffer[] buffers){
		if(canCallback&&pages.size()==0){
			callbackQueue(CallbackType.BUFFER,currentBuffers,null);
			return;
		}
		for(ByteBuffer buffer:buffers){
			int length=buffer.remaining();
			currentLength+=length;
			putLength+=length;
			currentBuffers.add(buffer);
		}
		if(borderLength>0&&currentLength>=borderLength){
			int pageId=pagePool.pushPage(BuffersUtil.toByteBufferArray(currentBuffers));
			currentLength=0;
			currentBuffers.clear();
			pages.add(pageId);
		}
	}
	
	/* この処理でIOが走行してはいけない
	 * この処理からcallbackされる可能性はある
	 */
	public void putBuffer(ByteBuffer[] buffers){
		putBufferInternal(buffers);
		callback();
	}
	
	private synchronized void nextBufferInternal(){
		if(canCallback){
			return;
		}
		if(pages.size()==0){
			if(currentLength!=0){
				callbackQueue(CallbackType.BUFFER,currentBuffers,null);
				currentBuffers.clear();
				currentLength=0;
			}else{
				canCallback=true;
			}
			return;
		}
		int pageId=pages.getLast();
		ByteBuffer[] bufs=pagePool.popPage(pageId,true);
		if(bufs==null){
			canCallback=true;
			return;
		}
		pages.removeLast();
		List<ByteBuffer> result=new ArrayList<ByteBuffer>();
		BuffersUtil.addByteBufferList(result, bufs);
		while(pages.size()!=0){
			Integer pid=pages.getLast();
			bufs=pagePool.popPage(pid,false);
			if(bufs==null){
				break;
			}
			pages.removeLast();
			BuffersUtil.addByteBufferList(result, bufs);
		}
		if(pages.size()==0&&currentLength!=0){
			result.addAll(currentBuffers);
			currentBuffers.clear();
			currentLength=0;
		}
		callbackQueue(CallbackType.BUFFER,result,null);
	}
	
	public void nextBuffer(){
		nextBufferInternal();
		callback();
	}
	
	private synchronized void endBufferInternal(){
		while(pages.size()!=0){
			int pageId=pages.removeLast();
			pagePool.removePage(pageId);
		}
		if(currentLength!=0){
			PoolManager.poolBufferInstance(currentBuffers);
			currentBuffers.clear();
			currentLength=0;
		}
		canCallback=false;
		callbackQueue(CallbackType.END,null,null);
	}
	
	public void endBuffer(){
		endBufferInternal();
		callback();
	}
	
	private synchronized void onPageInInternal(int pageId,ByteBuffer[] buffers){
		if(!canCallback){
			callbackQueue(CallbackType.FAILURE,null,new Throwable());
			return;
		}
		int lastPageId=pages.getLast();
		if(lastPageId!=pageId){
			callbackQueue(CallbackType.FAILURE,null,new Throwable());
			return;
		}
		pages.removeLast();
		List<ByteBuffer> result=new ArrayList<ByteBuffer>();
		BuffersUtil.addByteBufferList(result, buffers);
		while(pages.size()!=0){
			Integer pid=pages.getLast();
			ByteBuffer[] bufs=pagePool.popPage(pid,false);
			if(bufs==null){
				break;
			}
			pages.removeLast();
			BuffersUtil.addByteBufferList(result, bufs);
		}
		if(pages.size()==0&&currentLength!=0){
			result.addAll(currentBuffers);
			currentBuffers.clear();
			currentLength=0;
		}
		callbackQueue(CallbackType.BUFFER,result,null);
	}
	
	public void onPageIn(int pageId,ByteBuffer[] buffers){
		onPageInInternal(pageId,buffers);
		callback();
	}
	
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
	private ByteBuffer[] cbBuffers;
	private Throwable cbFailure;
	private boolean isCallbackProcessing=false;
	void callbackQueue(CallbackType type,List<ByteBuffer>cbBuffers,Throwable cbFailure){
		Log.debug(logger,"callbackQueue sid:",sid);
		if(cbBuffers!=null){
			if(this.cbBuffers!=null){
				logger.error("duplicate cbBuffers");
			}
			this.cbBuffers=BuffersUtil.toByteBufferArray(cbBuffers);
		}
		if(cbFailure!=null){
			if(this.cbFailure!=null){
				logger.error("duplicate cbFailure");
			}
			this.cbFailure=cbFailure;
		}
		synchronized(callbackQueue){
			callbackQueue.addLast(type);
		}
	}
	
	void callback(){
		Log.debug(logger,"callback sid:",sid);
		CallbackType type;
		synchronized(callbackQueue){
			if(isCallbackProcessing || callbackQueue.size()<=0){
				Log.debug(logger,"callback loopout sid:",getPoolId());
				return;
			}
			isCallbackProcessing=true;
			type=callbackQueue.removeFirst();
		}
		while(true){
			switch(type){
			case BUFFER:
				boolean next=false;
				callbackLength+=BuffersUtil.remaining(cbBuffers);
				try{
					next=bufferGetter.onBuffer(cbBuffers, userContext);
				}catch(Throwable t){
					logger.error("bufferGetter.onBuffer error",t);
				}
				cbBuffers=null;
				if(next){
					nextBufferInternal();
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
	}
}
