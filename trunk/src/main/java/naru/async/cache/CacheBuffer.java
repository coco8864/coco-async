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
 * 機能としては、Sotreと重複するが以下の点で異なる
 * 1)ファイルを直接読み出すことができる
 * 2)writeモード時に先頭bufferを保持するので、Header/Data形式のデータ構造にフィットする
 * 
 * IOを発生を極力避けるポリシ
 * cacheには、BUF_SIZE単位で登録する。きりのよくないところではそのままcacheせず、ファイルから読み出し時にcache
 * readに関しては順次アクセスをサポート
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
	
	/* bufferは消費される */
	public static CacheBuffer open(ByteBuffer[] buffer){
		CacheBuffer asyncFile=(CacheBuffer)PoolManager.getInstance(CacheBuffer.class);
		asyncFile.useCache=false;//
		asyncFile.isReadMode=true;//
		asyncFile.topBuffer=buffer;//
		asyncFile.length=BuffersUtil.remaining(buffer);
		return asyncFile;
	}

	/* read modeでのopen */
	public static CacheBuffer open(File file){
		return open(file,true);
	}

	/* read modeでのopen */
	/* 再利用の可能性がないファイルはcacheを使わない */
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
		}else{//ファイル出力しなかった
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
		inAsyncRead=false;/* asyncReadの再帰呼び出しをチェックするフラグ */
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
	/* 自力で読み込む場合に利用する */
	private java.nio.channels.FileChannel fileChannel;
	private long position=0;//読み出し時,offset計算は呼び出し側で行う事
	private boolean inAsyncRead=false;
	private boolean useCache;
	private boolean isReadMode=false;
	//write modeからはじめた場合は、topBufferが設定される,データはこれが最後の可能性もある
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
					/* この先でTimer処理になる、推奨しない */
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
	
	/* topBufferに全てが含まれるか */
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
				File file=File.createTempFile("AsyncFile","dat");//TODO dir指定
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
			if(inAsyncRead){//このまま呼ぶと無限ループに落ちる
				//onBufferが到着しないのにasyncBufferを呼び出した等
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
		if(inAsyncRead){//callbackからasyncReadが呼ばれた、この呼び出しは推奨しない,処理が無意味に遅くなる
			TimerManager.setTimeout(0, this, new Object[]{bufferGetter,offset,userContext});
			return false;
		}
		inAsyncRead=true;//このmethodから復帰する際必ずfalseに変更する
		//終端の判断
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
		if(useCache){//cacheを使うか?
			//cacheに存在するか？
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
		//cacheに登録
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
