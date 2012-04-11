package naru.async.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.async.timer.TimerManager;

/**
 * IO�𔭐����ɗ͔�����|���V
 * read�Ɋւ��Ă͏����A�N�Z�X���T�|�[�g
 * @author Owner
 *
 */
public class AsyncFile extends PoolBase implements Timer{
	private static FileCache fileCache=FileCache.getInstance();
	private static BufferCache bufferCache=BufferCache.getInstance();
	
	/* write mode */
	public static AsyncFile open(){
		AsyncFile asyncFile=(AsyncFile)PoolManager.getInstance(AsyncFile.class);
		asyncFile.useCache=true;//
		asyncFile.isReadMode=false;//
		return asyncFile;
	}

	/* read mode�ł�open */
	public static AsyncFile open(File file){
		return open(file,true);
	}

	/* read mode�ł�open */
	/* �ė��p�̉\�����Ȃ��t�@�C����cache���g��Ȃ� */
	public static AsyncFile open(File file,boolean useCache){
		AsyncFile asyncFile=(AsyncFile)PoolManager.getInstance(AsyncFile.class);
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
		}
		isReadMode=true;
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
		position=0;
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
	private long position=0;
	private boolean inAsyncRead=false;
	private boolean useCache;
	private boolean isReadMode=false;
	//write mode����͂��߂��ꍇ�́AtopBuffer���ݒ肳���,�f�[�^�͂��ꂪ�Ō�̉\��������
	private ByteBuffer[] topBuffer=null;
	private File createTmpFile=null;
	
	public FileInfo getFileInfo(){
		return fileInfo;
	}
	
	public void position(long position){
		this.position=position;
	}
	
	public ByteBuffer[] getTopBuffer(){
		return topBuffer;
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
					asyncRead(bufferGetter,userContext);
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
	
	public void write(ByteBuffer buffer){
		write(BuffersUtil.toByteBufferArray(buffer));
	}
	
	public synchronized void write(ByteBuffer[] buffer){
		if(isReadMode){
			throw new IllegalStateException("write asyncRead");
		}
		long length=BuffersUtil.remaining(buffer);
		if(topBuffer==null){
			topBuffer=buffer;
			position+=length;
			return;
		}
		try {
			if(fileChannel==null){
				File file=File.createTempFile("AsyncFile","dat");//TODO dir�w��
				fileInfo=fileCache.createFileInfo(file);
				if(useCache){
					bufferCache.put(fileInfo,0,topBuffer);
				}
				FileOutputStream fos=new FileOutputStream(fileInfo.getFile());
				fileChannel=fos.getChannel();
				fileChannel.write(PoolManager.duplicateBuffer(topBuffer));
			}
			if(useCache){
				bufferCache.put(fileInfo,position,buffer);
			}
			fileChannel.write(buffer);
			position+=length;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	/* ��{�I��BufferGetter�C�x���g����asyncRead�͌Ăяo���Ȃ��� */
	public synchronized boolean asyncRead(BufferGetter bufferGetter,Object userContext){
		if(!isReadMode){
			throw new IllegalStateException("AsyncFile asyncRead");
		}
		if(inAsyncRead){//callback����asyncRead���Ă΂ꂽ�A���̌Ăяo���͐������Ȃ�,���������Ӗ��ɒx���Ȃ�
			TimerManager.setTimeout(0, this, new Object[]{bufferGetter,userContext});
			return false;
		}
		inAsyncRead=true;//����method���畜�A����ەK��false�ɕύX����
		//�I�[�̔��f
		if(position>=fileInfo.length()){
			callback(TYPE_ONBUFFER_END,bufferGetter,userContext,null,null);
			return true;
		}
		ByteBuffer[] buffer=null;
		if(useCache){//cache���g����?
			//cache�ɑ��݂��邩�H
			buffer=bufferCache.get(fileInfo,position);
			if(buffer!=null){
				position+=BuffersUtil.remaining(buffer);
				callback(TYPE_ONBUFFER,bufferGetter,userContext,buffer,null);
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
			fileChannel.position(position);
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
			bufferCache.put(fileInfo,position,buffer);
		}
		position+=(long)length;
		callback(TYPE_ONBUFFER,bufferGetter,userContext,buffer,null);
		return true;
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
		asyncRead((BufferGetter)params[0],params[1]);
	}
}