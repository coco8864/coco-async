package naru.async.store;

import static org.junit.Assert.*;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.BuffersTester;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.store.Store;
import naru.async.timer.TimerManager;

public class PutGetTester implements BufferGetter,Timer{
	private long size;
	private long interval;
	
	private long putSize=0;
	private long getSize=0;
	private BuffersTester putTester=new BuffersTester();
	private BuffersTester getTester=new BuffersTester();
	
	private boolean isRun=false;
	private boolean isEnd=false;
	private Throwable error=null;
	
	//
	/**
	 *1:送信側がclose
	 *2:送信がasyncClose
	 *3:受信側が全データを受信したところでclose
	 *4:受信側が全データを受信したところでasyncClose
	 */
	private int closeType;
	
	PutGetTester(long size,long interval,int closeType){
		this.size=size;
		this.interval=interval;
		this.closeType=closeType;
	}
	
	public void run(){
		Store store=Store.open(false);
		TimerManager.setTimeout(interval, this, store);
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
		if(getSize>=size){
			if(closeType==3){
				store.close();
			}else if(closeType==4){
				store.close(this, store);
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
		synchronized(this){
			isEnd=true;
			notifyAll();
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
		if(closeType==1){
			store.close();
		}else if(closeType==2){
			store.close(this, store);
		}
	}
}
