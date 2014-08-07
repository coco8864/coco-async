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
	
	public enum ST {
		block,
		writable,
		writing,
		close
	}
	private ST st=ST.writable;
	
	
	//setup�Őݒ肳��recycle�����܂ŕێ�����
	private ChannelContext context;
	private Store store;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	
	private boolean isContextUnref=false;
	private long onBufferLength;
	
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
		PoolManager.poolBufferInstance(workBuffer);
		if(store!=null){
			store.unref();
			store=null;
		}
		prepareWriteReturnNull=false;
	}
	
	public void setup(){
		logger.debug("setup().cid:"+context.getPoolId()+":workBuffer:"+workBuffer);
		onBufferLength=0;
		isContextUnref=false;
		store=Store.open(false);//store�͂����ł����ݒ肵�Ȃ�
		store.ref();//store�������I����Ă����̃I�u�W�F�N�g�������Ă���ԕێ�����
		context.ref();//store�������Ă����context���m�ۂ���
		store.asyncBuffer(this, store);
	}
	
	public void cleanup(){
		logger.debug("cleanup.cid:"+context.getPoolId());
		if(store!=null){
			store.close(this,store);
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
				//������prepareWrite���Ăяo���ꂽ�ꍇ
				logger.debug("there is writeOrder but no buffer?cid:"+context.getPoolId()+ ":onBufferLength:"+onBufferLength);
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
//				int bufid=System.identityHashCode(buffer);
//				logger.info("doneWrite this:"+System.identityHashCode(this)+":bufid:"+bufid);
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
//		logger.info("onBuffer this:"+System.identityHashCode(this)+":bufsid:"+System.identityHashCode(buffer));
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
//				int bufid=System.identityHashCode(buffer[i]);
//				logger.info("onBuffer this:"+System.identityHashCode(this)+":bufid:"+bufid);
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
			//write��block���l������ꍇ
			context.queueuSelect();
			//write��block����̂��l�����Ȃ��ꍇ
			/*
			if( context.queueIO(ChannelContext.IO.WRITABLE)==false ){
				context.queueuSelect();
			}
			�������L���ɂ���ƈȉ��̗�O���ł�悤�ɂȂ���
2011-12-28 23:20:29,130 [thread-dispatch:1] ERROR naru.async.store.Page - buf.put error.offset:0:length:8:allocBufferSize:16384
java.nio.BufferOverflowException
	at java.nio.HeapByteBuffer.put(HeapByteBuffer.java:165)
	at naru.async.store.Page.putBytes(Page.java:456)
	at naru.async.store.Page.putBytes(Page.java:444)
	at naru.aweb.http.HeaderParser.getHeaderBuffer(HeaderParser.java:922)
	at naru.aweb.http.HeaderParser.getHeaderBuffer(HeaderParser.java:829)
	at naru.aweb.http.WebServerHandler.flushFirstResponse(WebServerHandler.java:674)
	at naru.aweb.http.WebServerHandler.responseEnd(WebServerHandler.java:356)
	at naru.aweb.handler.FileSystemHandler.responseBodyChannel(FileSystemHandler.java:352)
			get�����΂����ByteBuffer�����Ɏg���Ă���!!!
			*/
		}
		return false;
	}

	public void onBufferEnd(Object userContext) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			logger.debug("onBufferEnd.store!=userContext cid:"+context.getPoolId()+":store:"+store);//���Ȃ��Ǝv��
			return;
		}
		synchronized(this){
			if(isContextUnref){
				logger.error("duplicate WriterBuffer#onBufferEnd",new Throwable());
				return;
			}
			isContextUnref=true;
			logger.debug("onBufferEnd.cid:"+context.getPoolId());//���Ȃ��Ǝv��
			context.unref();//store���I�������̂�context�͊J�����Ă��悢
		}
	}
	
	public void onBufferFailure(Object userContext, Throwable falure) {
		if(store!=userContext){//callback�����O��close���ꂽ�ꍇ
			return;
		}
		synchronized(this){
			if(isContextUnref){
				logger.error("duplicate WriterBuffer#onBufferFailure",new Throwable());
//				logger.error("duplicate WriterBuffer#onBufferFailure prev",unrefStack);
				return;
			}
			isContextUnref=true;
//			unrefStack=new Throwable();
			logger.warn("onBufferFailure falure.cid:"+context.getPoolId(),falure);//���Ȃ��Ǝv��
			logger.warn("onBufferFailure now",new Exception());
			context.failure(falure);
			context.unref();//store���I�������̂�context�͊J�����Ă��悢
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