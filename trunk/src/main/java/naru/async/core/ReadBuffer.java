package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class ReadBuffer implements BufferGetter {
	private static Logger logger=Logger.getLogger(ReadBuffer.class);
	private static final String STORE_CREANUP="storeCreanup";
	
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private ChannelContext context;
	//setup�Őݒ肳��recycle�����܂ŕێ�����
	private boolean isContextUnref=false;
	
	private Store store;
	private long onBufferLength;
	
	/* 0������M�����ꍇ�́A���܂�putBuffer���ꂽ�Sbuffer��ԋp������A
	 * ���̎���asyncRead��onClose�ŕ��A������B*/
	private boolean isDisconnect=false;//������؂ꂽ�ꍇ
	
	private synchronized void setStore(Store store){
		if(store!=null){
			context.ref();
			store.ref();
		}
		Store orgStore=this.store;
		this.store=store;
		if(orgStore!=null){
			context.unref();
			orgStore.unref();
		}
	}
	
	public ReadBuffer(ChannelContext context){
		this.context=context;
	}
	
	public void dump(){
		dump(logger);
	}
	public void dump(Logger logger){
		try {
			logger.debug("$realReadLength:"+store.getPutLength() + ":callbackLength:"+store.getGetLength());
			logger.debug("$workBuffer:"+workBuffer);
		} catch (RuntimeException e) {
			//workBuffer��toString���鎞��ConcurrentModificationException����������ꍇ������
		}
	}
	
	public void recycle() {
		Iterator<ByteBuffer> itr=workBuffer.iterator();
		while(itr.hasNext()){
			ByteBuffer buf=itr.next();
			PoolManager.poolBufferInstance(buf);
			itr.remove();
		}
		if(store!=null){
			store.unref();
			store=null;
		}
	}
	
	public void setup(){
		onBufferLength=0;
		isDisconnect=false;
		logger.debug("setup().cid:"+context.getPoolId()+":workBuffer:"+workBuffer);
		store=Store.open(false);//store�͂����ł����ݒ肵�Ȃ�
		store.ref();//store�������I����Ă����̃I�u�W�F�N�g�������Ă���ԕێ�����
		context.ref();//store�������Ă����context���m�ۂ���
		isContextUnref=false;
//		setStore(Store.open(false));
	}
	
	public void cleanup(){
		logger.debug("cleanup.cid:"+context.getPoolId());
		if(store!=null){
			store.close(this,store);
//			setStore(null);
		}
	}
	
	//�������read����buffer�́AputBuffers���\�b�h�ŋl�ߍ���
	public void putBuffer(ByteBuffer[] buffer){
		if(store==null || store.isCloseReceived()){
			logger.warn("store closed.cid:"+context.getPoolId()+":store:"+store);
			PoolManager.poolBufferInstance(buffer);
			return;
		}
		logger.debug("pubBuffer.cid:"+context.getPoolId()+":bufSize:"+BuffersUtil.remaining(buffer));
		store.putBuffer(buffer);
	}
	
	public void disconnect(){
		logger.debug("disconnect.cid:"+context.getPoolId());
		isDisconnect=true;
	}
	
	//�ȉ����������낤��callback����B
	//1)�������ǂݍ���buffer������B
	//2)read Order������
	//ChannelContext��ioLock�̒�����Ăяo����邽�߂Q�d�ɑ��s���鎖�͂Ȃ��B
	public boolean callback(/*ContextOrders orders*/){
		logger.debug("callback.cid:"+context.getPoolId()+":isDisconnect:"+isDisconnect);
		boolean doneDisconnect=false;
		synchronized(workBuffer){//workBuffer�����
			int size=workBuffer.size();
			if(size!=0){
				ByteBuffer[] buffer=(ByteBuffer[])workBuffer.toArray(BuffersUtil.newByteBufferArray(size));
				long bufSize=BuffersUtil.remaining(buffer);
				if(context.ordersDoneRead(buffer)){
					logger.debug("callback.ordersDoneRead.cid:" + context.getPoolId()+ ":bufSize:" + bufSize +":hashCode:"+buffer.hashCode());
					workBuffer.clear();
					return true;
				}
				PoolManager.poolArrayInstance(buffer);//�z���ԋp
				//buffer�͂��邯��asyncRead���N�G�X�g���Ȃ�
				if(isDisconnect){
					logger.debug("isDisconnect and not asyncRead.cid:"+context.getPoolId()+":"+buffer.length);
					//����callback�������ǂ܂�asyncRead���Ă��Ȃ��^�C�~���O�ŉ�����؂�鎖������
					//doneDisconnect=true;
				}
//				return false;
			}else if(store==null||(store.getPutLength()==onBufferLength && isDisconnect)){
				//������؂�Ă���A����M�������ׂẴf�[�^��ʒm����
				logger.debug("doneDisconnect.cid:"+context.getPoolId());
				doneDisconnect=true;
			}else if(isDisconnect){
				//����͐ؒf����Ă��邪�AStore�ɂ܂��f�[�^���c���Ă���
				logger.debug("isDisconnect but not doneDisconnect.cid:"+context.getPoolId()+ ":store.getPutLength():"+store.getPutLength() +":onBufferLength:" +onBufferLength);
//				return store.asyncBuffer(this, store);
			}
		}
		if(doneDisconnect){
			context.doneClosed(false);
			return true;
		}
		return store.asyncBuffer(this, store);
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffer) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			return false;
		}
//		ContextOrders orders=(ContextOrders)userContext;
		boolean	doneDisconnect=false;
		synchronized(workBuffer){//workBuffer�����
			onBufferLength+=BuffersUtil.remaining(buffer);
			for(int i=0;i<buffer.length;i++){
				workBuffer.add(buffer[i]);
			}
			//�z���ԋp
			PoolManager.poolArrayInstance(buffer);
			int size=workBuffer.size();
			ByteBuffer[] readBuffer=(ByteBuffer[])workBuffer.toArray(BuffersUtil.newByteBufferArray(size));
			long bufSize=BuffersUtil.remaining(readBuffer);
			if(context.ordersDoneRead(readBuffer)){
				workBuffer.clear();
				logger.debug("onBuffer ordersDoneRead return true cid:"+ context.getPoolId()+ ":store:"+store+":bufSize:"+bufSize+":size:"+workBuffer.size());
			}else{
				PoolManager.poolArrayInstance(readBuffer);//�z���ԋp
				logger.debug("onBuffer return false cid:"+ context.getPoolId()+ ":store:"+store+":buffer:"+BuffersUtil.remaining(buffer)+":size:"+workBuffer.size());
				return false;
			}
			/* callback������������؂�Ă����ꍇ�AdoneClosed�����{����K�v������ */
			if(store==null||(store.getPutLength()==onBufferLength && isDisconnect)){
				//������؂�Ă���A����M�������ׂẴf�[�^��ʒm����
				logger.debug("doneDisconnect.cid:"+context.getPoolId());
				doneDisconnect=true;
			}else if(isDisconnect){
				//����͐ؒf����Ă��邪�AStore�ɂ܂��f�[�^���c���Ă���
				logger.debug("isDisconnect but not doneDisconnect.cid:"+context.getPoolId()+ ":store.getPutLength():"+store.getPutLength() +":onBufferLength:" +onBufferLength);
			}
		}
		if(doneDisconnect){//store��null�̏ꍇ�A�K������doneDisconnect=true
			context.doneClosed(false);
			return true;
		}
		return store.asyncBuffer(this, store);
	}
	
	public void onBufferEnd(Object userContext) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			return;
		}
		synchronized(this){
			if(isContextUnref){
				logger.error("duplicate ReadBuffer#onBufferEnd",new Throwable());
			}
			isContextUnref=true;
//			ContextOrders orders=(ContextOrders)userContext;
			logger.debug("onBufferEnd.cid:"+context.getPoolId());//���Ȃ��Ǝv��
//			setStore(null);
			context.unref();//store���I�������̂�context�͊J�����Ă��悢
		}
	}
	
	public void onBufferFailure(Object userContext, Throwable falure) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			return;
		}
		synchronized(this){
			if(isContextUnref){
				logger.error("duplicate ReadBuffer#onBufferFailure",new Throwable());
			}
			isContextUnref=true;
//			ContextOrders orders=(ContextOrders)userContext;
			logger.warn("onBufferFailure falure",falure);//���Ȃ��Ǝv��
			logger.warn("onBufferFailure now",new Exception());
			context.failure(falure);
			context.unref();//store���I�������̂�context�͊J�����Ă��悢
//			setStore(null);
		}
	}

	public long getOnBufferLength() {
		return onBufferLength;
	}
}
