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
 * �X�P�W���[���ɂ���������buffer��asyncWrite���邽�߂̃N���X
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
	private WriteScheduler prevSceduler;//���̊�����҂���write�����s����
	private long timerId;
	private long scheduleWriteTime;//�ڕW�������ݎ���
	private long actualWriteTime;//���ۂ̏������ݎ���
	
	
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
	
	/* �P�ɒx�����ď������ޏꍇ */
	public long scheduleWrite(long writeTime,ChannelHandler handler,Object userContext,ByteBuffer[] buffer){
		return scheduleWrite(handler,userContext,buffer,writeTime,0,null);
	}
	
	/* ��ssceduler�̎��ɒx�����ď������ޏꍇ */
	public long scheduleWrite(long writeTime,ChannelHandler handler,Object userContext,ByteBuffer[] buffer,WriteScheduler prevSceduler){
		return scheduleWrite(handler,userContext,buffer,writeTime,0,prevSceduler);
	}
	
	/**
	 * writeLength��0�̏ꍇ�A�Sbuffer�𑗐M�A���̌�close���Ȃ��B
	 * +�l�̏ꍇ�A���̒��������M�A�Sbuffer�ɖ����Ȃ��ꍇ�́A���̌�close����
	 * -�l�̏ꍇ�Abuffer�����炻�̒������������Ă��̌�close����
	 * (Long.MIN_VALUE��ݒ肵���ꍇ�A1�o�C�g���������܂���close����)
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
