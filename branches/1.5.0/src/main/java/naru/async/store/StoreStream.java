package naru.async.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public class StoreStream extends PoolBase implements BufferGetter {
	private boolean isComplete=false;
	private boolean result=false;
	private Throwable failure;
	private OutputStream os;//出力時だけで使用

	@Override
	public void recycle(){
		isComplete=false;
		result=false;
		failure=null;
		os=null;
	}
	
	public void setOutputStream(OutputStream os){
		this.os=os;
	}
	
	public boolean isComplete(){
		return isComplete;
	}
		
	public boolean getResult(){
		return result;
	}
		
	public Throwable getFailure(){
		return failure;
	}
		
	public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
		if(os==null){//出力か入力かを切り分けるためにに使用
			return true;
		}
		try {
			BuffersUtil.toStream(buffers, os);
		} catch (IOException e) {
			Store store=(Store)userContext;
			store.close();
			synchronized(this){
				result=false;
				this.failure=e;
				notify();
			}
			return false;
		}
		return true;
	}
	public void onBufferEnd(Object userContext) {
		synchronized(this){
			isComplete=true;
			result=true;
			notify();
		}
	}
	public void onBufferFailure(Throwable failure, Object userContext) {
		synchronized(this){
			isComplete=true;
			result=false;
			this.failure=failure;
			notify();
		}
	}
	
	public static String streamToStore(InputStream is) throws IOException{
		Store store=Store.open(true);
		store.getStoreId();
		while(true){
			ByteBuffer buffer=BuffersUtil.toBuffer(is);
			if(buffer==null){
				break;
			}
			buffer.flip();
			store.putBuffer(BuffersUtil.toByteBufferArray(buffer));
		}
		StoreStream helper=(StoreStream)PoolManager.getInstance(StoreStream.class);
		String digest=null;
		synchronized(helper){
			store.close(helper, store);
			digest=store.getDigest();
			while(!helper.isComplete()){
				try {
					helper.wait();
				} catch (InterruptedException ignore) {
				}
			}
			if(!helper.getResult()){
				throw new RuntimeException("fail to streamToStore.",helper.getFailure());
			}
		}
		helper.unref(true);
		return digest;
	}
	
	public static void storeToStream(long storeId,OutputStream os) throws IOException{
		Store store=Store.open(storeId);
		StoreStream helper=(StoreStream)PoolManager.getInstance(StoreStream.class);
		helper.setOutputStream(os);
		store.asyncBuffer(helper, store);
		synchronized(helper){
			while(!helper.isComplete()){
				try {
					helper.wait();
				} catch (InterruptedException ignore) {
				}
			}
			if(!helper.getResult()){
				throw new RuntimeException("fail to storeToStream.",helper.getFailure());
			}
		}
		helper.unref(true);
	}
}
