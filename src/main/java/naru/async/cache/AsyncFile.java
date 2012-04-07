package naru.async.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.Timer;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.async.timer.TimerManager;

/**
 * IOを発生を極力避けるポリシ
 * readに関しては順次アクセスをサポート
 * @author Owner
 *
 */
public class AsyncFile extends PoolBase implements Timer{
	private static FileCache fileCache=FileCache.getInstance();
	private static BufferCache bufferCache=BufferCache.getInstance();

	public static AsyncFile open(File file){
		return open(file,true);
	}

	/* 再利用の可能性がないファイルはcacheを使わない */
	public static AsyncFile open(File file,boolean useCache){
		AsyncFile asyncFile=(AsyncFile)PoolManager.getInstance(AsyncFile.class);
		asyncFile.init(file, useCache);
		return asyncFile;
	}
	
	private void init(File file,boolean useCache){
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
		inAsyncRead=false;
		position=0;
	}

	private FileInfo fileInfo;
	/* 自力で読み込む場合に利用する */
	private java.nio.channels.FileChannel fileChannel;
	private long position=0;
	private boolean inAsyncRead=false;
	private boolean useCache;
	
	public FileInfo getFileInfo(){
		return fileInfo;
	}
	
	/*
	public boolean isDirectory() {
		return fileInfo.isDirectory();
	}

	public boolean isFile() {
		return fileInfo.isFile();
	}

	public boolean isCanRead() {
		return fileInfo.isCanRead();
	}

	public boolean isExists() {
		return fileInfo.isExists();
	}

	public long getLastModified() {
		return fileInfo.getLastModified();
	}

	public long getLength() {
		return fileInfo.getLength();
	}

	//ここが使われるのは、listing機能のみ、この負荷を削減したければlisting機能をoffにする
	public File[] listFiles() {
		return fileInfo.listFiles();
	}
	*/
	public void position(long position){
		this.position=position;
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

	/* 基本的にBufferGetterイベントからasyncReadは呼び出さない事 */
	public synchronized boolean asyncRead(BufferGetter bufferGetter,Object userContext){
		if(inAsyncRead){//callbackからasyncReadが呼ばれた、この呼び出しは推奨しない,処理が無意味に遅くなる
			TimerManager.setTimeout(0, this, new Object[]{bufferGetter,userContext});
			return false;
		}
		inAsyncRead=true;//このmethodから復帰する際必ずfalseに変更する
		//終端の判断
		if(position>=fileInfo.length()){
			callback(TYPE_ONBUFFER_END,bufferGetter,userContext,null,null);
			return true;
		}
		ByteBuffer[] buffer=null;
		if(useCache){//cacheを使うか?
			//cacheに存在するか？
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
		//cacheに登録
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
			if(inAsyncRead){//このまま呼ぶと無限ループに落ちる
				//onBufferが到着しないのにasyncBufferを呼び出した等
				((BufferGetter)params[0]).onBufferFailure(userContext, new IllegalStateException("AsyncFile secuence"));
				return;
			}
		}
		asyncRead((BufferGetter)params[0],params[1]);
	}
}
