package naru.async.pool;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import naru.async.pool.ByteBufferLife.SearchKey;

import org.apache.log4j.Logger;

public class ByteArrayLife extends ReferenceLife {
	private static Logger logger=Logger.getLogger(ByteArrayLife.class);
	private static final int FREE_LIFE_MAX = 16;
	private byte[] array;
	
	/*ByteBuffer���L�[��ByteBufferLife���������鎞�Ɏg�� */
	private SearchKey searchKey=new SearchKey();//userLifes�̃��b�N���ɗ��p
	//����seed�̎�ReferenceLife
	private Set<ByteBufferLife> userLifes=new HashSet<ByteBufferLife>();
	//freeLifes���Ǘ�����ByteBuffer��GC����Ȃ��悤��value�ŎQ�Ƃ��Ă���
	private Map<ByteBufferLife,ByteBuffer> freeLifes=new HashMap<ByteBufferLife,ByteBuffer>();
	
	public ByteArrayLife(ByteBuffer firstByteBuffer,Pool pool) {
		super(firstByteBuffer.array());
		setPool(pool);
		array=firstByteBuffer.array();
		ByteBufferLife byteBufferLife=new ByteBufferLife(firstByteBuffer,this);
		freeLifes.put(byteBufferLife,firstByteBuffer);
//		userLifes.add(byteBufferLife);
	}

	void gcInstance() {//������array�ւ̎Q�Ƃ�ێ����Ă���̂ŌĂ΂��킯���Ȃ�
	}

	/*�@����getBufferInstance�̉����ŌĂяo����鎖��z�� */
	ByteBuffer getFirstByteBuffer(ByteBuffer byteBuffer){
		synchronized(userLifes){
//			logger.debug("getOnlyByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(freeLifes.size()<1){
				logger.error("fail to getFirstByteBuffer no freeLifes",new IllegalStateException());
				return null;//pool�������Ă��������ł���A�Ăяo���҂͔�Q�ҁA�C���X�^���X�擾���Ď��s
			}
			searchKey.setByteBuffer(byteBuffer);
			if(byteBuffer!=freeLifes.remove(searchKey)){
				logger.error("fail to getFirstByteBuffer not in freeLifes:"+freeLifes.size(),new IllegalStateException());
				return null;//pool�������Ă��������ł���A�Ăяo���҂͔�Q�ҁA�C���X�^���X�擾���Ď��s
			}
			ByteBufferLife byteBufferLife=searchKey.getHit();
//			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
//			ByteBufferLife byteBufferLife=itr.next();
//			itr.remove();
			userLifes.add(byteBufferLife);
			byteBufferLife.ref();
//			ByteBuffer byteBuffer=(ByteBuffer)byteBufferLife.get();
			ref();
			return byteBuffer;
		}
	}
	
	/*�@pool������ĎQ�Ƃ��J������ꍇ�ɌĂяo����� */
	void releaseLife(){
		synchronized(userLifes){
//			logger.debug("getOnlyByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
			if(userLifes.size()!=0){
				throw new IllegalStateException("byteBufferLifes.size()="+userLifes.size());
			}
			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
			while(itr.hasNext()){
				ByteBufferLife byteBufferLife=itr.next();
				itr.remove();
				byteBufferLife.clear();
			}
		}
	}
	
	/*�@duplicate�̉����ŌĂяo����鎖��z�� */
	ByteBuffer getByteBuffer(){
		synchronized(userLifes){
			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
			if(itr.hasNext()){
				ByteBufferLife life=itr.next();
				itr.remove();
				userLifes.add(life);
				life.ref();
				ByteBuffer byteBuffer=(ByteBuffer)life.get();
				ref();
				return byteBuffer;
			}
			ByteBuffer byteBuffer=ByteBuffer.wrap(array);
			ByteBufferLife byteBufferLife=new ByteBufferLife(byteBuffer,this);
			byteBufferLife.ref();
//			logger.debug("getByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
			if(userLifes.add(byteBufferLife)==false){
				logger.error("byteBufferLifes.add return false",new Throwable());
			}
			ref();
			return byteBuffer;
		}
	}
	
	/* poolBufferInstance�̉����ŌĂяo����鎖��z�� */
	void poolByteBuffer(ByteBuffer buffer){
		synchronized(userLifes){
			ByteBufferLife byteBufferLife=removeByteBuffer(buffer);
			if(byteBufferLife==null){
				//2�dpoolBufferInstance()..
				logger.error("poolByteBuffer...",new Throwable());//TODO
				return;
			}
			byteBufferLife.unref();
			if(freeLifes.size()<FREE_LIFE_MAX){
				freeLifes.put(byteBufferLife,buffer);
			}else{
				/* pool�ɏ\������̂ł��̃C���X�^���X�͎̂Ă� */
				byteBufferLife.clear();//clear���Ă�queue����邱�Ƃ�����...�Ȃ�???
			}
			if(unref()){
				if(userLifes.size()!=0){
					throw new IllegalStateException("byteBufferLifes.size()="+userLifes.size());
				}
				//��\��ByteBuffer��pool�ɖ߂�
				//pool����ꂽ�ꍇ���̉�����releaseLife���\�b�h���Ăяo�����
				pool.poolInstance(buffer);
				return;
			}
			if(getRef()==0){//pool���ɂ���
				//2�dpoolBufferInstance()..
				logger.error("poolByteBuffer...getRef()==0",new Throwable());//TODO
				return;
			}
		}
	}
	
	private ByteBufferLife 	removeByteBuffer(ByteBuffer buffer){
//		logger.debug("removeByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
		synchronized(userLifes){
			searchKey.setByteBuffer(buffer);
			if(userLifes.remove(searchKey)){
				return searchKey.getHit();
			}
			/*
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			while(itr.hasNext()){
				ByteBufferLife life=itr.next();
				if( life.get()==buffer){
					itr.remove();
					return life;
				}
			}
			*/
		}
		return null;
	}
	
	/* gc�̉����ŌĂяo����鎖��z�� */
	void gcByteBufferLife(ByteBufferLife byteBufferLife){
		if(byteBufferLife.getRef()==0){
			//clear���Ă�ReferenceQueue�ɒʒm����鎖������
			return;
		}
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet+":BBLsize:"+userLifes.size()+":byteBufferLife:"+byteBufferLife,stackOfGet);
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(byteBufferLife.timeOfGet))+":get thread:"+byteBufferLife.threadNameOfGet);
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(byteBufferLife.timeOfPool))+":pool thread:"+byteBufferLife.threadNameOfPool,byteBufferLife.stackOfPool);
		logger.warn("gcByteBufferLife.getInstance get:"+byteBufferLife.get());
		
		synchronized(userLifes){
			if( userLifes.remove(byteBufferLife)==false ){
				throw new IllegalStateException();
			}
			byteBufferLife.unref();
			byteBufferLife.clear();
			if(unref()){
				/* ��\��ByteBuffer����� */
				ByteBuffer byteBuffer=ByteBuffer.wrap(array);
				byteBufferLife=new ByteBufferLife(byteBuffer,this);
				freeLifes.put(byteBufferLife,byteBuffer);
				pool.poolInstance(byteBuffer);//ByteBuffer��pool�ɖ߂�
			}
		}
	}
	
	public void info(){
		info(false);
	}
	
	public void info(boolean isDetail){
		Object[] bfls=userLifes.toArray();
//		Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
		logger.debug("array:"+array +":ref:"+ getRef());
		for(int i=0;i<bfls.length;i++){
			ByteBufferLife bbl=(ByteBufferLife)bfls[i];
			if(isDetail){
				logger.debug("referent:"+bbl.get() +":refCount:"+bbl.getRef()+":getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+bbl.threadNameOfGet,bbl.stackOfGet);
			}else{
				logger.debug("referent:"+bbl.get() +":refCount:"+bbl.getRef()+":getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+bbl.threadNameOfGet/*,stackOfGet*/);
			}
		}
	}
	
	@Override
	public String toString(){
		return "$$ByteArrayLife." +array.length;
	}
}
