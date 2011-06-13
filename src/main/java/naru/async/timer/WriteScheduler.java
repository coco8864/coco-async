package naru.async.timer;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

/**
 * �X�P�W���[���ɂ���������buffer��asyncWrite���邽�߂̃N���X
 * �d�����I�������A�����ŉ������
 * @author Naru
 */
public class WriteScheduler extends PoolBase implements Timer{
	private static Logger logger = Logger.getLogger(WriteScheduler.class);
	private ChannelHandler handler;
	private ByteBuffer[] buffer;
	private Object userContext;
	boolean isCloseEnd;
	boolean isDoneWrite;
	private WriteScheduler prevSceduler;//���̊�����҂���write�����s����
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
		unref();//�d�����I������̂Ŏ��������
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
		unref();//�d�����I������̂Ŏ��������
	}
}
