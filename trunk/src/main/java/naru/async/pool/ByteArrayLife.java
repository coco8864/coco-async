package naru.async.pool;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

public class ByteArrayLife extends ReferenceLife {
	static private Logger logger=Logger.getLogger(ByteArrayLife.class);
	
	private byte[] array;
	private Set<ByteBufferLife> byteBufferLifes=new HashSet<ByteBufferLife>();//����seed�̎�ReferenceLife
	
	public ByteArrayLife(ByteBuffer firstByteBuffer,Pool pool) {
		super(firstByteBuffer.array());
		setPool(pool);
		array=firstByteBuffer.array();
		ByteBufferLife byteBufferLife=new ByteBufferLife(firstByteBuffer,this);
//		synchronized(byteBufferLifes){//���������Ȃ̂ŎQ�Ƃ��Ă���l�����Ȃ��͂�
			byteBufferLifes.add(byteBufferLife);
//		}
	}

	void gcInstance() {//������array�ւ̎Q�Ƃ�ێ����Ă���̂ŌĂ΂��킯���Ȃ�
	}

	/*�@getBufferInstance�̉����ŌĂяo����鎖��z�� */
	/* �B�ꎝ���Ă���͂���ByteBuffer��ԋp,Pool����Ƃ�ꂽ�΂��肾����ByteBuffer�́A1�̂͂� */
	ByteBuffer getOnlyByteBuffer(){
		synchronized(byteBufferLifes){
//			logger.debug("getOnlyByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(byteBufferLifes.size()!=1){
				throw new IllegalStateException("byteBufferLifes.size()="+byteBufferLifes.size());
			}
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			ByteBufferLife byteBufferLife=itr.next();
			byteBufferLife.ref();
			ByteBuffer byteBuffer=(ByteBuffer)byteBufferLife.get();
			ref();
			return byteBuffer;
		}
	}
	
	/*�@getBufferInstance�̉����ŌĂяo����鎖��z�� */
	/* �B�ꎝ���Ă���͂���ByteBuffer��ԋp,Pool����Ƃ�ꂽ�΂��肾����ByteBuffer�́A1�̂͂� */
	ByteBufferLife getOnlyByteBufferLife(){
		synchronized(byteBufferLifes){
			logger.debug("getOnlyByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(byteBufferLifes.size()!=1){
				throw new IllegalStateException("byteBufferLifes.size()="+byteBufferLifes.size());
			}
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			ByteBufferLife byteBufferLife=itr.next();
			return byteBufferLife;
		}
	}
	
	
	/*�@duplicate�̉����ŌĂяo����鎖��z�� */
	ByteBuffer getByteBuffer(){
		ByteBuffer byteBuffer=ByteBuffer.wrap(array);
		ByteBufferLife byteBufferLife=new ByteBufferLife(byteBuffer,this);
		byteBufferLife.ref();
		synchronized(byteBufferLifes){
			logger.debug("getByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			byteBufferLifes.add(byteBufferLife);
			ref();
		}
		return byteBuffer;
	}
	
	/* poolBufferInstance�̉����ŌĂяo����鎖��z�� */
	void poolByteBuffer(ByteBuffer buffer){
		synchronized(byteBufferLifes){
//			logger.debug("poolByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(unref()){
				if(byteBufferLifes.size()!=1){
					throw new IllegalStateException("byteBufferLifes.size()="+byteBufferLifes.size());
				}
				Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
				ByteBufferLife byteBufferLife=itr.next();
				byteBufferLife.unref();
				pool.poolInstance(buffer);
				return;
			}
			if(getRef()==0){//pool���ɂ���
				//2�dpoolBufferInstance()..
				logger.error("poolByteBuffer...getRef()==0",new Throwable());//TODO
				return;
			}
			ByteBufferLife byteBufferLife=removeByteBuffer(buffer);
			if(byteBufferLife==null){
				//2�dpoolBufferInstance()..
				logger.error("poolByteBuffer...",new Throwable());//TODO
				return;
			}
			byteBufferLife.unref();
			byteBufferLife.clear();
		}
	}
	
	private ByteBufferLife 	removeByteBuffer(ByteBuffer buffer){
		synchronized(byteBufferLifes){
			logger.debug("removeByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			while(itr.hasNext()){
				ByteBufferLife life=itr.next();
				if( life.get()==buffer){
					itr.remove();
					return life;
				}
			}
		}
		return null;
	}
	
	/* gc�̉����ŌĂяo����鎖��z�� */
	void gcByteBufferLife(ByteBufferLife byteBufferLife){
		logger.warn("gcByteBufferLife.getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet,stackOfGet);
		pool.gcLife(byteBufferLife);
		synchronized(byteBufferLifes){
			if( byteBufferLifes.remove(byteBufferLife)==false ){
				throw new IllegalStateException();
			}
			byteBufferLife.unref();
			byteBufferLife.clear();
			if(unref()){
				//�Ō��ByteBufferLife��GC���ꂽ�ꍇ�́A�E�E�E
				ByteBuffer byteBuffer=ByteBuffer.wrap(array);
				byteBufferLife=new ByteBufferLife(byteBuffer,this);
				byteBufferLife.ref();
				synchronized(byteBufferLifes){
					byteBufferLifes.add(byteBufferLife);
				}
				pool.poolInstance(byteBuffer);
				return;
			}
		}
	}
	
	public void info(){
		info(false);
	}
	
	public void info(boolean isDetail){
		Object[] bfls=byteBufferLifes.toArray();
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
}
