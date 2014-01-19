package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class WriteBuffer implements BufferGetter {
	private static Logger logger=Logger.getLogger(WriteBuffer.class);
	private ChannelContext context;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private int workSize=8092;
	
	//setup�Őݒ肳��recycle�����܂ŕێ�����
	private Store store;
	private long onBufferLength;
	private long curBufferLength;
	
	public WriteBuffer(ChannelContext context){
		this.context=context;
	}
	public void dump(){
		dump(logger);
	}
	
	public void dump(Logger logger){
		try {
			logger.debug("$aplWriteLength:"+store.getPutLength() + ":realWriteLength:"+store.getGetLength());
			logger.debug("$workBuffer.size:"+workBuffer.size());
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
		curBufferLength=0;
		if(store!=null){
			store.unref();
			store=null;
		}
	}
	
	public void setup(){
		logger.debug("setup().cid:"+context.getPoolId()+":workBuffer:"+workBuffer);
		onBufferLength=0;
		store=Store.open(false);//store��recycle�܂ŕێ���������
		store.ref();//store�������I����Ă����̃I�u�W�F�N�g�������Ă���ԕێ�����
		context.ref();//store�������Ă����context���m�ۂ���
		store.asyncBuffer(this, store);
	}
	
	public void cleanup(){
		logger.debug("cleanup.cid:"+context.getPoolId());
		store.close(this,store);
	}
	
	//�A�v����������buffer�́AputBuffers�ŋl�ߍ���
	public void putBuffer(ByteBuffer[] buffer){
		logger.debug("putBuffer cid:"+ context.getPoolId()+":store:"+store +":len:"+BuffersUtil.remaining(buffer));
		store.putBuffer(buffer);
	}
	
	//queueIO���鎞��workBuffer�����邱�Ƃ��m�F���Ă���̂ŕK��workBuffer�͂���B
	public ByteBuffer[] prepareWrite(){
		logger.debug("prepareWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		ByteBuffer[] buffer=null;
		synchronized(this){
			buffer=BuffersUtil.toByteBufferArray(workBuffer);
		}
		//����Buffer��v������
		store.asyncBuffer(this, store);
		return buffer;
	}
	
	//�������݌�A�S��������buffer�̓��T�C�N���ɂ܂킷
	public void doneWrite(ByteBuffer[] prepareBuffers){
		logger.debug("doneWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		PoolManager.poolArrayInstance(prepareBuffers);//prepare�ŏ�������Buffer�z��͕ԋp
		synchronized(this){
			Iterator<ByteBuffer> itr=workBuffer.iterator();
			while(itr.hasNext()){
				ByteBuffer buffer=itr.next();
				if( buffer.hasRemaining() ){
					break;
				}
				itr.remove();
				PoolManager.poolBufferInstance(buffer);
			}
			curBufferLength=BuffersUtil.remaining(workBuffer);
		}
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffer) {
		boolean next=false;
		long len=BuffersUtil.remaining(buffer);
		synchronized(this){
			onBufferLength+=len;
			curBufferLength+=len;
			for(int i=0;i<buffer.length;i++){
				workBuffer.add(buffer[i]);
			}
			//�z���ԋp
			PoolManager.poolArrayInstance(buffer);
			if(curBufferLength<workSize){
				next=true;
			}
		}
		//TODO write��block���Ă��Ȃ����,writeQ��
		if(next){
			return true;
		}else{
			return false;
		}
	}

	public void onBufferEnd(Object userContext) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			logger.debug("onBufferEnd.store!=userContext cid:"+context.getPoolId()+":store:"+store);//���Ȃ��Ǝv��
			return;
		}
		synchronized(this){
			if(context==null){
				logger.error("duplicate WriterBuffer#onBufferEnd",new Throwable());
				return;
			}
			logger.debug("onBufferEnd.cid:"+context.getPoolId());//���Ȃ��Ǝv��
			context.unref();//store���I�������̂�context�͊J�����Ă��悢
			context=null;
		}
	}
	
	public void onBufferFailure(Object userContext, Throwable falure) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			return;
		}
		synchronized(this){
			if(context==null){
				logger.error("duplicate WriterBuffer#onBufferFailure",new Throwable());
				return;
			}
			logger.warn("onBufferFailure falure.cid:"+context.getPoolId(),falure);//���Ȃ��Ǝv��
			logger.warn("onBufferFailure now",new Exception());
			context.failure(falure);
			context.unref();//store���I�������̂�context�͊J�����Ă��悢
			context=null;
		}
	}
	
	/**
	 * asyncWrite�ɓn���ꂽbuffer�̒���
	 * @return
	 */
	public long getPutLength() {
		if(store!=null){
			return store.getPutLength();
		}
		return onBufferLength;
	}
}
