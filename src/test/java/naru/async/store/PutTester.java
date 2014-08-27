package naru.async.store;

import static org.junit.Assert.*;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.BuffersTester;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.store.Store;
import naru.async.store.StoreManager;
import naru.async.timer.TimerManager;

public class PutTester implements BufferGetter,Timer{
	private long size;
	private long interval;
	
	private long putSize=0;
	private long getSize=0;
	private BuffersTester putTester=new BuffersTester();
	
	private boolean isRun=false;
	private boolean isEnd=false;
	private Throwable error=null;
	
	private long storeId;
	
	public long getStoreId(){
		return storeId;
	}
	
	/**
	 *1:送信側後close(true)
	 *2:送信側後close(false)
	 *3:送信側後asyncClose
	 */
	private int closeType;
	
	//close falseのテスト
	//asyncCloseのテスト
	
	PutTester(long size,long interval,int closeType){
		this.size=size;
		this.interval=interval;
		this.closeType=closeType;
	}
	
	public void run(){
		Store store=Store.open(true);
		storeId=store.getStoreId();
		putSize=store.getPutLength();
		TimerManager.setTimeout(interval, this, store);
		isRun=true;
	}

	public void check() throws Throwable{
		if(error!=null){
			throw error;
		}
		if(closeType==1||closeType==3){
			if(putSize!=getSize){
				throw new Throwable("putSize:"+putSize + " getSize:"+getSize);
			}
		}
//		getTester.check();
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

	public void onTimer(Object userContext) {
		Store store=(Store)userContext;
		if(putSize<size){
			ByteBuffer[] buffers=putTester.getBuffers();
			BuffersUtil.flipBuffers(buffers);
			putSize+=BuffersUtil.remaining(buffers);
			store.putBuffer(buffers);
			TimerManager.setTimeout(interval, this, store);
			return;
		}
		if(closeType==3){
			store.close(this, store);
			return;
		}else if(closeType==1){
//			long storeId=store.getStoreId();
			store.close(true);
			String digest=store.getDigest();
			while(true){
				getSize=StoreManager.getStoreLength(digest);
				if(getSize>=0){
					break;
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}else if(closeType==2){
			store.close(true,true);
		}
		synchronized(this){
			isEnd=true;
			notifyAll();
		}
	}

	//asyncCloseを利用した場合に使う
	public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
		return false;
	}

	public void onBufferEnd(Object userContext) {
		Store store=(Store)userContext;
		getSize=store.getPutLength();
		synchronized(this){
			isEnd=true;
			notifyAll();
		}
	}

	public void onBufferFailure(Throwable falure, Object userContext) {
		error=new Throwable("onBufferFailure",falure);
		synchronized(this){
			isEnd=true;
			notifyAll();
		}
	}
}
