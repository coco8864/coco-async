package naru.async.store;

import static org.junit.Assert.*;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.BuffersTester;
import naru.async.pool.BuffersUtil;
import naru.async.store.Store;

public class GetTester implements BufferGetter{
//	private long size;
//	private long interval;
	
	private long putSize=0;
	private long getSize=0;
//	private BuffersTester putTester=new BuffersTester();
	private BuffersTester getTester=new BuffersTester();
	
	private long storeId;
	private boolean isRun=false;
	private boolean isEnd=false;
	private Throwable error=null;
	
	//
	/**
	 *1:終端まで読む
	 *2:終端まで読む
	 *3:受信側が全データを受信したところでclose
	 *4:受信側が全データを受信したところでasyncClose
	 */
	private int closeType;
	
	GetTester(long storeId,int closeType){
//		this.size=size;
//		this.interval=interval;
		this.storeId=storeId;
		this.closeType=closeType;
	}
	
	public void run(){
		Store store=Store.open(storeId);
		putSize=store.getPutLength();
		store.asyncBuffer(this, store);
		isRun=true;
	}

	public void check() throws Throwable{
		if(error!=null){
			throw error;
		}
		getTester.check();
		if(putSize!=getSize){
			throw new Throwable("putSize:"+putSize + " getSize:"+getSize);
		}
	}
	
	public synchronized void waitForEnd(){
		if(isRun==false){
			run();
		}
		while(true){
			if(isEnd){
				break;
			}
			try {
				wait();
			} catch (InterruptedException ignore) {
			}
		}
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		Store store=(Store)userContext;
		getSize+=BuffersUtil.remaining(buffers);
		getTester.putBuffer(buffers);
		if(getSize>=putSize){
			if(closeType==3){
				store.close();
			}else if(closeType==4){
				store.close(this, store);
//				store.asyncClose(this, store);
			}
		}
		return true;
	}

	public void onBufferEnd(Object userContext) {
		synchronized(this){
			isEnd=true;
			notifyAll();
		}
	}

	public void onBufferFailure(Object userContext, Throwable falure) {
		error=new Throwable("onBufferFailure",falure);
	}
}
