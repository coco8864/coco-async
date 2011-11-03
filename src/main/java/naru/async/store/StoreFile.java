package naru.async.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;

public class StoreFile {
	private static Logger logger=Logger.getLogger(StoreFile.class);
	private int readerCount;
	private ArrayBlockingQueue<FileChannel> readChannelsQueue;
	private ArrayBlockingQueue<FileChannel> writeChannelsQueue;
	private long length;
	
	private FileChannel popReadChannel(){
		try {
			return readChannelsQueue.take();
		} catch (InterruptedException e) {
			logger.error("popReadChannel error.",e);
		}
		return null;
	}
	
	private FileChannel popWriteChannel(){
		try {
			return writeChannelsQueue.take();
		} catch (InterruptedException e) {
			logger.error("popWriteChannel error.",e);
		}
		return null;
	}
	
	private void pushReadChannel(FileChannel readChannel){
		readChannelsQueue.offer(readChannel);
	}
	
	private void pushWriteChannel(FileChannel writeChannel){
		writeChannelsQueue.offer(writeChannel);
	}
	
	public StoreFile(File storeFile,int readerCount) throws IOException{
		if(!storeFile.exists()){
			File parent=storeFile.getParentFile();
			if(!parent.exists()){
				parent.mkdirs();
			}
		}
		this.readerCount=readerCount;
		length=storeFile.length();
		readChannelsQueue=new ArrayBlockingQueue<FileChannel>(readerCount);
		writeChannelsQueue=new ArrayBlockingQueue<FileChannel>(1);
		RandomAccessFile raf=new RandomAccessFile(storeFile,"rwd");
//		FileOutputStream fos=new FileOutputStream(storeFile,true);
		FileChannel writeChannel=raf.getChannel();
		writeChannel.position(length);//positionを一番後ろに持っていく
		writeChannelsQueue.offer(writeChannel);
		for(int i=0;i<readerCount;i++){
			RandomAccessFile rafrd=new RandomAccessFile(storeFile,"r");
			readChannelsQueue.offer(rafrd.getChannel());
//			FileInputStream fis=new FileInputStream(storeFile);
//			readChannelsQueue.offer(fis.getChannel());
		}
	}

	public long length(){
		return length;
	}
	
	public void read(ByteBuffer buffer,long offset){
		long bufferLength=buffer.remaining();
//		logger.debug("read:offset:"+offset +":length:"+bufferLength);
		FileChannel readChannel=null;
		try {
			readChannel=popReadChannel();
			readChannel.position(offset);
			long readLength=0;
			while(bufferLength>readLength){
				long len=readChannel.read(buffer);
				if(len<0){
					logger.warn("read short1.offset:"+offset +":bufferLength:"+bufferLength + ":readLength:"+readLength);
					return;
				}
				readLength+=len;
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			pushReadChannel(readChannel);
		}
	}
	
	public void read(ByteBuffer[] buffer,long offset){
		long bufferLength=BuffersUtil.remaining(buffer);
		logger.debug("read:offset:"+offset +":length:"+bufferLength);
		FileChannel readChannel=null;
		try {
			readChannel=popReadChannel();
			readChannel.position(offset);
			long readLength=0;
			while(bufferLength>readLength){
				long len=readChannel.read(buffer);
				if(len<0){
					logger.warn("read short2.offset:"+offset +":bufferLength:"+bufferLength + ":readLength:"+readLength);
					return;
				}
				readLength+=len;
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			pushReadChannel(readChannel);
		}
	}
	
	public long write(ByteBuffer buffer){
		long bufferLength=buffer.remaining();
		FileChannel writeChannel=null;
		try {
			writeChannel=popWriteChannel();
			long offset=writeChannel.position();
			long writeLength=0;
			while(bufferLength<=writeLength){
				writeLength+=writeChannel.write(buffer);
			}
			length=writeChannel.position();
			return offset;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			PoolManager.poolBufferInstance(buffer);
			pushWriteChannel(writeChannel);
		}
	}
	
	public long write(ByteBuffer[] buffer){
		long bufferLength=BuffersUtil.remaining(buffer);
		FileChannel writeChannel=null;
		long writeLength=0;
		int loopCount=0;
		try {
			writeChannel=popWriteChannel();
			long offset=writeChannel.position();
			while(bufferLength>writeLength){
				writeLength+=writeChannel.write(buffer);
				loopCount++;
			}
			length=writeChannel.position();
			return offset;
		} catch (IOException e) {
			/*
			 * http://msdn.microsoft.com/ja-jp/library/cc429856.aspx
			 * 曰く
			 * 未処理の非同期 I/O 要求が大量に残っているとき、WriteFile 関数は失敗することがあります。このような障害が発生した場合、GetLastError 関数は、ERROR_INVALID_USER_BUFFER または ERROR_NOT_ENOUGH_MEMORY を返すことがあります。
			 * どうすればよいのだろう
			 */
			logger.error("write fail.bufferLength:"+bufferLength+
					":writeLength:"+writeLength +
					":bufferCount:"+buffer.length +
					":loopCount:"+loopCount+
					":remaining:"+BuffersUtil.remaining(buffer),e);
			throw new IllegalStateException("write fail.bufferLength:"+bufferLength+":writeLength:"+writeLength +":loopCount:"+loopCount+":writeChannel:"+writeChannel,e);
		}finally{
			PoolManager.poolBufferInstance(buffer);
			pushWriteChannel(writeChannel);
		}
	}
	
	public long write(ByteBuffer buffer,long offset){
		long bufferLength=buffer.remaining();
		logger.debug("write:offset:"+offset +":length:"+bufferLength);
		FileChannel writeChannel=null;
		try {
			writeChannel=popWriteChannel();
			writeChannel.position(offset);
			long writeLength=0;
			while(bufferLength>writeLength){
				writeLength+=writeChannel.write(buffer);
			}
			if(writeChannel.position()>length){
				length=writeChannel.position();
			}
			return offset;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			PoolManager.poolBufferInstance(buffer);
			pushWriteChannel(writeChannel);
		}
	}
	
	public long write(ByteBuffer[] buffer,long offset){
		long bufferLength=BuffersUtil.remaining(buffer);
		FileChannel writeChannel=null;
		try {
			writeChannel=popWriteChannel();
			writeChannel.position(offset);
			long writeLength=0;
			while(bufferLength>writeLength){
				writeLength+=writeChannel.write(buffer);
			}
			if(writeChannel.position()>length){
				length=writeChannel.position();
			}
			return offset;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			PoolManager.poolBufferInstance(buffer);
			pushWriteChannel(writeChannel);
		}
	}
	
	/**
	 * ファイルサイズを0にするメソッド
	 */
	public void truncate(){
		//誰も読み込まないように設定
		FileChannel[] readers=new FileChannel[readerCount];
		for(int i=0;i<readerCount;i++){
			readers[i]=popReadChannel();
		}
		FileChannel writeChannel=popWriteChannel();
		try {
			//ファイルを0に
			writeChannel.truncate(0);
			length=0;
		} catch (IOException e) {
			logger.error("fail to truncate(0)",e);
			throw new IllegalStateException("fail to truncate(0)");
		}finally{
			for(int i=0;i<readerCount;i++){
				pushReadChannel(readers[i]);
			}
			pushWriteChannel(writeChannel);
		}
	}
	
	public void close(){
		try {
			while(true){
				FileChannel channel=readChannelsQueue.poll();
				if(channel==null){
					break;
				}
				channel.close();
			}
			while(true){
				FileChannel channel=writeChannelsQueue.poll();
				if(channel==null){
					break;
				}
				channel.close();
			}
		} catch (IOException e) {
			logger.error("close error.",e);
		}
	}
}
