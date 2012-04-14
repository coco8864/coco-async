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
 * 機能としては、Sotreと重複するが以下の点で異なる
 * 1)ファイルを直接読み出すことができる
 * 2)writeモード時に先頭bufferを保持するので、Header/Data形式のデータ構造にフィットする
 * 
 * IOを発生を極力避けるポリシ
 * readに関しては順次アクセスをサポート
 * @author Owner
 *
 */
public class AsyncBuffer extends PoolBase implements Timer{
	private static FileCache fileCache=FileCache.getInstance();
	private static BufferCache bufferCache=BufferCache.getInstance();
	
	/* write mode */
	public static AsyncBuffer open(){
		AsyncBuffer asyncFile=(AsyncBuffer)PoolManager.getInstance(AsyncBuffer.class);
		asyncFile.useCache=true;//
		asyncFile.isReadMode=false;//
		return asyncFile;
	}

	/* read modeでのopen */
	public static AsyncBuffer open(File file){
		return open(file,true);
	}

	/* read modeでのopen */
	/* 再利用の可能性がないファイルはcacheを使わない */
	public static AsyncBuffer open(File file,boolean useCache){
		AsyncBuffer asyncFile=(AsyncBuffer)PoolManager.getInstance(AsyncBuffer.class);
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
		dataLength=fileInfo.length();
	}
	
	/* write modeからreadModeに切り替え */
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
		if(createTmpFile!=null){
			createTmpFile.delete();
			createTmpFile=null;
		}
		isReadMode=true;
		dataLength=position;
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
		inAsyncRead=false;/* asyncReadの再帰呼び出しをチェックするフラグ */
		dataLength=position=0;
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
	/* 自力で読み込む場合に利用する */
	private java.nio.channels.FileChannel fileChannel;
	private long position=0;
	private boolean inAsyncRead=false;
	private boolean useCache;
	private boolean isReadMode=false;
	//write modeからはじめた場合は、topBufferが設定される,データはこれが最後の可能性もある
	private ByteBuffer[] topBuffer=null;
	private File createTmpFile=null;
	private long dataLength;
	
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
					/* この先でTimer処理になる、推奨しない */
					asyncGet(bufferGetter,position,userContext);
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
	
	/* topBufferに全てが含まれるか */
	public boolean isInTopBuffer(){
		if(!inAsyncRead||topBuffer==null){
			return false;
		}
		return BuffersUtil.remaining(topBuffer)==dataLength;
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
		ByteBuffer[] dup=null;
		try {
			if(fileChannel==null){
				File file=File.createTempFile("AsyncFile","dat");//TODO dir指定
				fileInfo=fileCache.createFileInfo(file);
				if(useCache){
					bufferCache.put(fileInfo,0,topBuffer);
				}
				FileOutputStream fos=new FileOutputStream(fileInfo.getFile());
				fileChannel=fos.getChannel();
				dup=PoolManager.duplicateBuffers(topBuffer);
				fileChannel.write(dup);
				PoolManager.poolBufferInstance(dup);
				dup=null;
			}
			if(useCache){
				bufferCache.put(fileInfo,position,buffer);
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
	
	public synchronized boolean asyncGet(BufferGetter bufferGetter,Object userContext){
		return asyncGet(bufferGetter,position,userContext);
	}

	/* 基本的にBufferGetterイベントからasyncReadは呼び出さない事 */
	public synchronized boolean asyncGet(BufferGetter bufferGetter,long offset,Object userContext){
		if(!isReadMode){
			throw new IllegalStateException("AsyncFile asyncRead");
		}
		if(inAsyncRead){//callbackからasyncReadが呼ばれた、この呼び出しは推奨しない,処理が無意味に遅くなる
			TimerManager.setTimeout(0, this, new Object[]{bufferGetter,userContext});
			return false;
		}
		inAsyncRead=true;//このmethodから復帰する際必ずfalseに変更する
		//終端の判断
		if(offset>=dataLength){
			callback(TYPE_ONBUFFER_END,bufferGetter,userContext,null,null);
			return true;
		}
		ByteBuffer[] buffer=null;
		if(useCache){//cacheを使うか?
			//cacheに存在するか？
			buffer=bufferCache.get(fileInfo,offset);
			if(buffer!=null){
				position=offset+BuffersUtil.remaining(buffer);
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
			fileChannel.position(offset);
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
		//cacheに登録
		if(useCache){
			bufferCache.put(fileInfo,offset,buffer);
		}
		position=offset+(long)length;
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
			if(inAsyncRead){//このまま呼ぶと無限ループに落ちる
				//onBufferが到着しないのにasyncBufferを呼び出した等
				((BufferGetter)params[0]).onBufferFailure(userContext, new IllegalStateException("AsyncFile secuence"));
				return;
			}
		}
		asyncGet((BufferGetter)params[0],position,params[1]);
	}
}
