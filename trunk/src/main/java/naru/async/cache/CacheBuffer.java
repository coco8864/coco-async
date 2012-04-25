package naru.async.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.AsyncBuffer;
import naru.async.BufferGetter;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.async.timer.TimerManager;

/**
 * �@�\�Ƃ��ẮASotre�Əd�����邪�ȉ��̓_�ňقȂ�
 * 1)�t�@�C���𒼐ړǂݏo�����Ƃ��ł���
 * 2)write���[�h���ɐ擪buffer��ێ�����̂ŁAHeader/Data�`���̃f�[�^�\���Ƀt�B�b�g����
 * 
 * IO�𔭐����ɗ͔�����|���V
 * cache�ɂ́ABUF_SIZE�P�ʂœo�^����B����̂悭�Ȃ��Ƃ���ł͂��̂܂�cache�����A�t�@�C������ǂݏo������cache
 * read�Ɋւ��Ă͏����A�N�Z�X���T�|�[�g
 * @author Owner
 *
 */
public class CacheBuffer extends PoolBase implements AsyncBuffer,Timer{
	private static FileCache fileCache=FileCache.getInstance();
	private static BufferCache bufferCache=BufferCache.getInstance();
	private static long BUF_SIZE=PoolManager.getDefaultBufferSize();
	
	/* write mode */
	public static CacheBuffer open(){
		CacheBuffer asyncFile=(CacheBuffer)PoolManager.getInstance(CacheBuffer.class);
		asyncFile.useCache=true;//
		asyncFile.isReadMode=false;//
		return asyncFile;
	}
	
	/* buffer�͏����� */
	public static CacheBuffer open(ByteBuffer[] buffer){
		CacheBuffer asyncFile=(CacheBuffer)PoolManager.getInstance(CacheBuffer.class);
		asyncFile.useCache=false;//
		asyncFile.isReadMode=true;//
		asyncFile.topBuffer=buffer;//
		asyncFile.length=BuffersUtil.remaining(buffer);
		return asyncFile;
	}

	/* read mode�ł�open */
	public static CacheBuffer open(File file){
		return open(file,true);
	}

	/* read mode�ł�open */
	/* �ė��p�̉\�����Ȃ��t�@�C����cache���g��Ȃ� */
	public static CacheBuffer open(File file,boolean useCache){
		CacheBuffer asyncFile=(CacheBuffer)PoolManager.getInstance(CacheBuffer.class);
		asyncFile.init(file, useCache);
		return asyncFile;
	}
	
	private void init(File file,boolean useCache){
		isReadMode=true;
		if(useCache && fileCache.useCache()){
			this.useCache=true;
		}else{
			this.useCache=false;
		}
		if(this.useCache){
			fileInfo=fileCache.get(file);
		}else{
			fileInfo=fileCache.createFileInfo(file);
		}
		length=fileInfo.length();
	}
	
	/* write mode����readMode�ɐ؂�ւ� */
	public void flip(){
		if(isReadMode){
			throw new IllegalStateException("AsyncFile flip");
		}
		if(fileChannel!=null){
			try {
				fileChannel.close();
			} catch (IOException ignore) {
			}
			fileChannel=null;
		}else{//�t�@�C���o�͂��Ȃ�����
		}
		if(createTmpFile!=null){
			createTmpFile.delete();
			createTmpFile=null;
		}
		isReadMode=true;
		length=position;
		position=0;
	}
	
	@Override
	public void recycle() {
		if(fileChannel!=null){
			try {
				fileChannel.close();
			} catch (IOException ignore) {
			}
			fileChannel=null;
		}
		if(fileInfo!=null){
			fileInfo.unref();
			fileInfo=null;
		}
		inAsyncRead=false;/* asyncRead�̍ċA�Ăяo�����`�F�b�N����t���O */
		length=position=0;
		isReadMode=false;
		if(topBuffer!=null){
			PoolManager.poolBufferInstance(topBuffer);
			topBuffer=null;
		}
		if(createTmpFile!=null){
			createTmpFile.delete();
			createTmpFile=null;
		}
	}

	private FileInfo fileInfo;
	/* ���͂œǂݍ��ޏꍇ�ɗ��p���� */
	private java.nio.channels.FileChannel fileChannel;
	private long position=0;//�ǂݏo����,offset�v�Z�͌Ăяo�����ōs����
	private boolean inAsyncRead=false;
	private boolean useCache;
	private boolean isReadMode=false;
	//write mode����͂��߂��ꍇ�́AtopBuffer���ݒ肳���,�f�[�^�͂��ꂪ�Ō�̉\��������
	private ByteBuffer[] topBuffer=null;
	private File createTmpFile=null;
	private long length;
	
	public FileInfo getFileInfo(){
		return fileInfo;
	}
	
	public void position(long position){
		this.position=position;
	}
	
	public ByteBuffer[] popTopBuffer(){
		ByteBuffer[] result=topBuffer;
		topBuffer=null;
		return result;
	}
	
	private static final int TYPE_ONBUFFER=1;
	private static final int TYPE_ONBUFFER_END=2;
	private static final int TYPE_ONBUFFER_FAILURE=3;
	private void callback(int type,BufferGetter bufferGetter,Object userContext,ByteBuffer[] buffer,Throwable failure){
		try{
			switch(type){
			case TYPE_ONBUFFER:
				if(bufferGetter.onBuffer(userContext, buffer)){
					/* ���̐��Timer�����ɂȂ�A�������Ȃ� */
					asyncBuffer(bufferGetter,position,userContext);
				}
				break;
			case TYPE_ONBUFFER_END:
				bufferGetter.onBufferEnd(userContext);
				break;
			case TYPE_ONBUFFER_FAILURE:
				bufferGetter.onBufferFailure(userContext, failure);
				break;
			}
		}finally{
			inAsyncRead=false;
		}
	}
	
	/* topBuffer�ɑS�Ă��܂܂�邩 */
	public boolean isInTopBuffer(){
		if(!isReadMode||topBuffer==null){
			return false;
		}
		return BuffersUtil.remaining(topBuffer)==length;
	}
	
	public void putBuffer(ByteBuffer buffer){
		putBuffer(BuffersUtil.toByteBufferArray(buffer));
	}
	
	public synchronized void putBuffer(ByteBuffer[] buffer){
		if(isReadMode){
			throw new IllegalStateException("write asyncRead");
		}
		long length=BuffersUtil.remaining(buffer);
		if(topBuffer==null){
			topBuffer=buffer;
			position+=length;
			return;
		}
		ByteBuffer[] dup=null;
		try {
			if(fileChannel==null){
				File file=File.createTempFile("AsyncFile","dat");//TODO dir�w��
				fileInfo=fileCache.createFileInfo(file);
				if(useCache){
//					if(position==BUF_SIZE){
						bufferCache.put(fileInfo,0,topBuffer);
//					}
				}
				FileOutputStream fos=new FileOutputStream(fileInfo.getFile());
				fileChannel=fos.getChannel();
				dup=PoolManager.duplicateBuffers(topBuffer);
				fileChannel.write(dup);
				PoolManager.poolBufferInstance(dup);
				dup=null;
			}
			if(useCache){
//				if(position%BUF_SIZE==0){
					bufferCache.put(fileInfo,position,buffer);
//				}
			}
			dup=buffer;
			fileChannel.write(buffer);
			PoolManager.poolBufferInstance(dup);
			dup=null;
			position+=length;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			if(dup!=null){
				PoolManager.poolBufferInstance(dup);
				dup=null;
			}
		}
	}
	
	public void close(){
		if(fileChannel!=null){
			try {
				fileChannel.close();
			} catch (IOException ignore) {
			}
			fileChannel=null;
		}
		unref();
	}
	
	public void onTimer(Object userContext) {
		Object[] params=(Object[])userContext;
		synchronized(this){
			if(inAsyncRead){//���̂܂܌ĂԂƖ������[�v�ɗ�����
				//onBuffer���������Ȃ��̂�asyncBuffer���Ăяo������
				((BufferGetter)params[0]).onBufferFailure(userContext, new IllegalStateException("AsyncFile secuence"));
				return;
			}
		}
		asyncBuffer((BufferGetter)params[0],(Long)params[1],params[2]);
	}

	public boolean asyncBuffer(BufferGetter bufferGetter, Object userContext) {
		return asyncBuffer(bufferGetter,position,userContext);
	}

	public boolean asyncBuffer(BufferGetter bufferGetter, long offset,Object userContext) {
		if(!isReadMode){
			throw new IllegalStateException("AsyncFile asyncRead");
		}
		if(inAsyncRead){//callback����asyncRead���Ă΂ꂽ�A���̌Ăяo���͐������Ȃ�,���������Ӗ��ɒx���Ȃ�
			TimerManager.setTimeout(0, this, new Object[]{bufferGetter,offset,userContext});
			return false;
		}
		inAsyncRead=true;//����method���畜�A����ەK��false�ɕύX����
		//�I�[�̔��f
		if(offset>=length){
			callback(TYPE_ONBUFFER_END,bufferGetter,userContext,null,null);
			return true;
		}
		//0 -> 0
		//1-> 0
		//1023 -> 0
		//1024 -> 1
		//1025 -> 1
		long sizeOffset=0;
		long skipSize=0;
		sizeOffset=(offset/BUF_SIZE)*BUF_SIZE;
		skipSize=offset-sizeOffset;
		ByteBuffer[] buffer=null;
		if(useCache){//cache���g����?
			//cache�ɑ��݂��邩�H
			buffer=bufferCache.get(fileInfo,sizeOffset);
			if(buffer!=null){
				position=sizeOffset+BuffersUtil.remaining(buffer);
				BuffersUtil.skip(buffer, skipSize);
				if(BuffersUtil.remaining(buffer)==0){
					PoolManager.poolBufferInstance(buffer);
					callback(TYPE_ONBUFFER_END,bufferGetter,userContext,null,null);
				}else{
					callback(TYPE_ONBUFFER,bufferGetter,userContext,buffer,null);
				}
				return true;
			}
		}
		ByteBuffer dst=null;
		int length;
		try {
			if(fileChannel==null){
				FileInputStream fis=new FileInputStream(fileInfo.getFile());
				fileChannel=fis.getChannel();
			}
			dst = PoolManager.getBufferInstance();
			fileChannel.position(sizeOffset);
			length = fileChannel.read(dst);
		} catch (IOException e) {
			callback(TYPE_ONBUFFER_FAILURE,bufferGetter,userContext,null,e);
			return true;
		}
		if(length<=0){
			callback(TYPE_ONBUFFER_END,bufferGetter,userContext,null,null);
			return true;
		}
		dst.flip();
		buffer=BuffersUtil.toByteBufferArray(dst);
		//cache�ɓo�^
		if(useCache){
			bufferCache.put(fileInfo,sizeOffset,buffer);
		}
		position=sizeOffset+(long)length;
		BuffersUtil.skip(buffer, skipSize);
		callback(TYPE_ONBUFFER,bufferGetter,userContext,buffer,null);
		return true;
	}

	public long bufferLength() {
		return length;
	}
}
