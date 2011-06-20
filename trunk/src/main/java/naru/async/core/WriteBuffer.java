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
	private static final String STORE_CREANUP="storeCreanup";
//	private Object isBufReqLock=new Object();
//	private boolean isBufReq=false;
	
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private ChannelContext context;
	private Store store;
	private long onBufferLength;
//	private MessageDigest messageDigest;

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
		prepareWriteReturnNull=false;
	}
	
	public void setup(){
		logger.debug("setup().cid:"+context.getPoolId()+":workBuffer:"+workBuffer);
		onBufferLength=0;
		setStore(Store.open(false));
		store.asyncBuffer(this, store);
	}
	
	public void cleanup(){
		logger.debug("cleanup.cid:"+context.getPoolId());
		if(store!=null){
			store.close(this,store);
//			setStore(null);
		}
	}
	
	//�A�v����������buffer�́AputBuffers�ŋl�ߍ���
	public void putBuffer(ByteBuffer[] buffer){
		logger.debug("putBuffer cid:"+ context.getPoolId()+":store:"+store +":len:"+BuffersUtil.remaining(buffer));
		store.putBuffer(buffer);
		//write�\�ɂȂ�̂�҂�
		/* �����Ɉȉ����R�����g�A�E�g onBuffer�ɂ���̂ŕK�v�Ȃ��񂶂�Ȃ����H */
		//context.queueuSelect();
	}
	
	private boolean prepareWriteReturnNull=false;
	//queueIO���鎞��workBuffer�����邱�Ƃ��m�F���Ă���̂ŕK��workBuffer�͂���B
	public ByteBuffer[] prepareWrite(){
		logger.debug("prepareWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		ByteBuffer[] buffer=null;
		synchronized(workBuffer){
			int size=workBuffer.size();
			if(size!=0){
				buffer=(ByteBuffer[])workBuffer.toArray(BuffersUtil.newByteBufferArray(size));
//				workBuffer.clear();
			}else{
				prepareWriteReturnNull=true;
				logger.debug("prepareWrite size=0 cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
				//IOManager��write���悤�Ƃ������o�b�t�@���܂�store�̒��ɂ��菀���ł��Ȃ��B
				//�����΂�onBuffer���Ă΂��͂�
			}
		}
		if(buffer==null){
			//�Q�d�ɂȂ邩������Ȃ����A����Buffer��v������
			if(store==null || store.getPutLength()==onBufferLength){
				logger.error("there is writeOrder but no buffer?cid:"+context.getPoolId()+ ":onBufferLength:"+onBufferLength);
			}
			if(store!=null){
				store.asyncBuffer(this, store);
			}
		}else{
			logger.debug("prepareWrite cid:"+ context.getPoolId() +":bufferSize:"+BuffersUtil.remaining(buffer));
		}
		return buffer;
	}
	
	//�������݌�A�S��������buffer�̓��T�C�N���ɂ܂킷
	public void doneWrite(ByteBuffer[] prepareBuffers){
		logger.debug("doneWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		PoolManager.poolArrayInstance(prepareBuffers);//prepare�ŏ�������Buffer�z��͕ԋp
		synchronized(workBuffer){
			Iterator<ByteBuffer> itr=workBuffer.iterator();
			while(itr.hasNext()){
				ByteBuffer buffer=itr.next();
				if( buffer.hasRemaining() ){
					break;
				}
				itr.remove();
				PoolManager.poolBufferInstance(buffer);
			}
			if(workBuffer.size()!=0){
				logger.debug("left workBuffer.size:"+workBuffer.size());
				//write�\�ɂȂ�̂�҂�
				//context.queueuSelect();
				return;
			}
			prepareWriteReturnNull=false;
		}
		//����Buffer��v������
		if(store!=null){
			store.asyncBuffer(this, store);
		}
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffer) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			return false;
		}
		logger.debug("onBuffer cid:"+ context.getPoolId()+ ":store:"+store+":buffer:"+BuffersUtil.remaining(buffer)+":size:"+workBuffer.size());
		boolean isQueueSelect=false;
		synchronized(workBuffer){
			onBufferLength+=BuffersUtil.remaining(buffer);
			if(workBuffer.size()==0&&buffer.length!=0){
				isQueueSelect=true;
			}
			for(int i=0;i<buffer.length;i++){
				workBuffer.add(buffer[i]);
			}
			//�z���ԋp
			PoolManager.poolArrayInstance(buffer);
			if(prepareWriteReturnNull){
				prepareWriteReturnNull=false;
				context.queueIO(ChannelContext.IO.WRITABLE);
			}
		}
		if(isQueueSelect){//write�\�ɂȂ�̂�҂�
			context.queueuSelect();
		}
		return false;
	}

	public void onBufferEnd(Object userContext) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			logger.debug("onBufferEnd.store!=userContext cid:"+context.getPoolId()+":store:"+store);//���Ȃ��Ǝv��
			return;
		}
		logger.debug("onBufferEnd.cid:"+context.getPoolId());//���Ȃ��Ǝv��
		setStore(null);
	}
	
	public void onBufferFailure(Object userContext, Throwable falure) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			return;
		}
		logger.warn("onBufferFailure falure.cid:"+context.getPoolId(),falure);//���Ȃ��Ǝv��
		logger.warn("onBufferFailure now",new Exception());
		context.failure(falure);
		setStore(null);
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
