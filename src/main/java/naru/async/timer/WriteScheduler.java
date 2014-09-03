package naru.async.timer;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.Log;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

/**
 * スケジュールにしたがってbufferをasyncWriteするためのクラス
 * @author Naru
 */
public class WriteScheduler extends PoolBase implements Timer{
	private static Logger logger = Logger.getLogger(WriteScheduler.class);
	private ChannelHandler handler;
	private ByteBuffer[] buffer;
	private Object userContext;
	boolean isCloseEndBeforeWrite;
	boolean isCloseEnd;
	boolean isDoneWrite;
	private WriteScheduler prevSceduler;//この完了を待ってwriteを実行する
	private long timerId;
	private long scheduleWriteTime;//目標書き込み時刻
	private long actualWriteTime;//実際の書き込み時刻
	
	
	public static WriteScheduler create(ChannelHandler handler,Object userContext,ByteBuffer[] buffer,long writeTime,long writeLength,WriteScheduler prevSceduler){
		WriteScheduler scheduler=(WriteScheduler) PoolManager.getInstance(WriteScheduler.class);
		scheduler.scheduleWrite(handler, userContext, buffer, writeTime, writeLength, prevSceduler);
		return scheduler;
	}

	public void recycle() {
		setHandler(null);
		setPrevSceduler(null);
		if(buffer!=null){
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
		isCloseEndBeforeWrite=isCloseEnd=false;
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
		buffer=null;
		userContext=null;
		synchronized(this){
			isDoneWrite=true;
			notifyAll();
		}
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
		if(writeLength==Long.MIN_VALUE){
			isCloseEndBeforeWrite=true;			
		}else if(writeLength<0){
			length+=writeLength;
			if(length<0){
				length=0;
			}
			BuffersUtil.cut(buffer,length);
			isCloseEnd=true;
		}else if(writeLength>0){
			if(writeLength<length){
				length=writeLength;
				isCloseEnd=true;
			}
			BuffersUtil.cut(buffer,length);
		}
		this.scheduleWriteTime=writeTime+System.currentTimeMillis();
//		long now=System.currentTimeMillis();
		Log.debug(logger,"writeTime=",(writeTime));
		if(writeTime<=0){
			if(prevSceduler!=null){
				writeTime=prevSceduler.scheduleWriteTime-System.currentTimeMillis();
			}
			if(writeTime<=0){
				onTimer(null);
				return length;
			}
		}
		timerId=TimerManager.setTimeout(writeTime,this,null);
		return length;
	}

	public void onTimer(Object timeoutUserContext) {
		Log.debug(logger,"onTimeout.cid",handler.getChannelId());
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
		if(isCloseEndBeforeWrite){
			handler.asyncClose(userContext);
		}else{
			handler.asyncWrite(buffer, userContext);
			actualWriteTime=System.currentTimeMillis();
			if(isCloseEnd){
				Log.debug(logger,"WriteScheduler asyncClose");
				handler.asyncClose(userContext);
			}
		}
		setHandler(null);
		buffer=null;
		userContext=null;
		synchronized(this){
			isDoneWrite=true;
			notifyAll();
		}
	}

	public long getActualWriteTime() {
		return actualWriteTime;
	}
	public long getScheduleWriteTime() {
		return scheduleWriteTime;
	}
}
