package naru.async.timer;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

/**
 * スケジュールにしたがってbufferをasyncWriteするためのクラス
 * 仕事が終わったら、自分で回収する
 * @author Naru
 */
public class WriteScheduler extends PoolBase implements Timer{
	private static Logger logger = Logger.getLogger(WriteScheduler.class);
	private ChannelHandler handler;
	private ByteBuffer[] buffer;
	private Object userContext;
	boolean isCloseEnd;
	boolean isDoneWrite;
	private WriteScheduler prevSceduler;//この完了を待ってwriteを実行する
	private long timerId;

	public void recycle() {
		setHandler(null);
		setPrevSceduler(null);
		if(buffer!=null){
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
		isCloseEnd=false;
		isDoneWrite=false;
		userContext=null;
		timerId=-1;
		super.recycle();
	}
	
	private void setPrevSceduler(WriteScheduler prevSceduler){
		if(prevSceduler!=null){
			prevSceduler.ref();
		}
		if(this.prevSceduler!=null){
			this.prevSceduler.unref();
		}
		this.prevSceduler=prevSceduler;
	}
	
	private void setHandler(ChannelHandler handler){
		if(handler!=null){
			handler.ref();
		}
		if(this.handler!=null){
			this.handler.unref();
		}
		this.handler=handler;
	}
	public void cancel(){
		if(timerId==-1 || !TimerManager.clearTimeout(timerId)){
			return;
		}
		if(prevSceduler!=null){
			prevSceduler.cancel();
			prevSceduler=null;
		}
		setHandler(null);
		PoolManager.poolBufferInstance(buffer);
		userContext=null;
		synchronized(this){
			isDoneWrite=true;
			notifyAll();
		}
		unref();//仕事が終わったので自分を回収
	}
	
	
	/* 単に遅延して書き込む場合 */
	public long scheduleWrite(long writeTime,ChannelHandler handler,Object userContext,ByteBuffer[] buffer){
		return scheduleWrite(handler,userContext,buffer,writeTime,0,null);
	}
	
	/* 先行scedulerの次に遅延して書き込む場合 */
	public long scheduleWrite(long writeTime,ChannelHandler handler,Object userContext,ByteBuffer[] buffer,WriteScheduler prevSceduler){
		return scheduleWrite(handler,userContext,buffer,writeTime,0,prevSceduler);
	}
	
	/**
	 * writeLengthが0の場合、全bufferを送信、その後closeしない。
	 * +値の場合、その長さ分送信、全bufferに満たない場合は、その後closeする
	 * -値の場合、buffer長からその長さ分を引いてその後closeする
	 * (Long.MIN_VALUEを設定した場合、1バイトも書き込まずにcloseする)
	 * @param handler
	 * @param buffer
	 * @param writeTime
	 * @param writeLength
	 */
	public long scheduleWrite(ChannelHandler handler,Object userContext,ByteBuffer[] buffer,long writeTime,long writeLength,WriteScheduler prevSceduler){
		setHandler(handler);
		this.buffer=buffer;
		this.userContext=userContext;
		setPrevSceduler(prevSceduler);
		long length=BuffersUtil.remaining(buffer);
		if(writeLength<0){
			length+=writeLength;
			if(length<0){
				length=0;
			}
			BuffersUtil.cut(buffer,length);
			isCloseEnd=true;
		}
		if(writeLength>0){
			if(writeLength<length){
				length=writeLength;
				isCloseEnd=true;
			}
			BuffersUtil.cut(buffer,length);
		}
		
		long now=System.currentTimeMillis();
		logger.debug("writeTime-now="+(writeTime-now));
		logger.debug("writeTime="+(writeTime));
		if(now>=writeTime){
			onTimer(null);
		}else{
			timerId=TimerManager.setTimeout(writeTime-now,this,null);
		}
		return length;
	}

	public void onTimer(Object timeoutUserContext) {
		logger.debug("onTimeout.cid"+handler.getChannelId());
		if(prevSceduler!=null){
			synchronized(prevSceduler){
				while(true){
					if(prevSceduler.isDoneWrite){
						break;
					}
					try {
						prevSceduler.wait();
					} catch (InterruptedException e) {
					}
				}
			}
			setPrevSceduler(null);
		}
		handler.asyncWrite(userContext, buffer);
		if(isCloseEnd){
			logger.debug("WriteScheduler asyncClose");
			handler.asyncClose(userContext);
		}
		setHandler(null);
		buffer=null;
		userContext=null;
		synchronized(this){
			isDoneWrite=true;
			notifyAll();
		}
		unref();//仕事が終わったので自分を回収
	}
}
