package naru.async.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

/**
 * IOを発生を極力避けるポリシ
 * readに関しては順次アクセスをサポート
 * @author Owner
 *
 */
public class AsyncFile extends PoolBase{
	private static FileCache fileCache=FileCache.getInstance();
	private static BufferCache bufferCache=BufferCache.getInstance();
	
	public static AsyncFile open(File file){
		AsyncFile asyncFile=(AsyncFile)PoolManager.getInstance(AsyncFile.class);
		asyncFile.fileInfo=fileCache.get(file);
		return asyncFile;
	}
	
	@Override
	public void recycle() {
		if(fileInfo!=null){
			fileInfo.unref();
			fileInfo=null;
		}
	}

	private FileInfo fileInfo;
	/* 自力で読み込む場合に利用する */
	private java.nio.channels.FileChannel fileChannel;
	private long position=0;
	
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

	/* callbackからasyncReadを呼び出すのは禁止,データが小さければ動作するが、
	 * 大きくなる呼び出し階層が許容できない程大きくなる */
	public synchronized boolean asyncRead(BufferGetter bufferGetter,Object userContext){
		//cacheに存在するか？
		ByteBuffer[] buffer=bufferCache.get(fileInfo,position);
		if(buffer!=null){
			bufferGetter.onBuffer(userContext, buffer);
			position+=BuffersUtil.remaining(buffer);
			return true;
		}
		ByteBuffer dst=null;
		int length;
		try {
			if(fileChannel==null){
				FileInputStream fis=new FileInputStream(fileInfo.getFile());
				fileChannel=fis.getChannel();
			}
			dst = PoolManager.getBufferInstance();
			length = fileChannel.read(dst);
		} catch (IOException e) {
			bufferGetter.onBufferEnd(userContext);
			return true;
		}
		dst.flip();
		buffer=BuffersUtil.toByteBufferArray(dst);
		position+=(long)length;
		//cacheに登録
		bufferCache.put(fileInfo,position,buffer);
		bufferGetter.onBuffer(userContext, buffer);
		return true;
	}
	
	public void close(){
		if(fileChannel==null){
			return;
		}
		try {
			fileChannel.close();
		} catch (IOException ignore) {
		}
		unref();
	}
}
