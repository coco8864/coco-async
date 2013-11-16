package naru.async;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.DataUtil;
import static org.junit.Assert.*;

/**
 * テストデータ送信側、受信側両方で利用する
 * seedが同じなら同じデータを生成する事を利用
 * @author naru
 *
 */
public class BufferGenerator {
	private static Logger logger=Logger.getLogger(BufferGenerator.class);
	private long length=0;
	private MessageDigest messageDigest;
	private ByteBuffer[] buffers;
	private String digest;
	
	
	public BufferGenerator(long length,long seed){
		this();
		this.length=length;
		this.digest=null;
		Random random=new Random(seed);
		buffers=BuffersUtil.prepareBuffers(length);
		for(int i=0;i<buffers.length;i++){
			random.nextBytes(buffers[i].array());
		}
		BuffersUtil.updateDigest(messageDigest,buffers);
	}
	public BufferGenerator(){
		isEnd=false;
		try {
			messageDigest=MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	public long getLength(){
		return length;
	}
	
	public void term(){
		PoolManager.poolBufferInstance(buffers);
		buffers=null;
	}
	
	public boolean check(long length,String digest){
		if(this.length!=length){
			return false;
		}
		if(this.digest==null){
			this.digest=DataUtil.digest(messageDigest);
		}
		if(!this.digest.equals(digest)){
			return false;
		}
		return true;
	}
	
	public String getDigest(){
		if(this.digest==null){
			this.digest=DataUtil.digest(messageDigest);
		}
		return digest;
	}
	
	private static class BufferIterator implements Iterator<ByteBuffer[]>{
		BufferIterator(BufferGenerator generator){
			this.generator=generator;
			this.index=0;
		}
		private BufferGenerator generator;
		private int index;
		public boolean hasNext() {
			return (index<generator.buffers.length);
		}

		public ByteBuffer[] next() {
			ByteBuffer buffer=PoolManager.duplicateBuffer(generator.buffers[index]);
			index++;
			return BuffersUtil.toByteBufferArray(buffer);
		}

		public void remove() {
			throw new UnsupportedOperationException("remove");
		}
	}
	
	public Iterator<ByteBuffer[]> bufferIterator(){
		return new BufferIterator(this);
	}
	
	public ByteBuffer[] getBuffer(){
		return PoolManager.duplicateBuffers(buffers);
	}
	
	
	public void put(ByteBuffer[] buffers){
		length+=BuffersUtil.remaining(buffers);
		System.out.println("length:"+length);
		BuffersUtil.updateDigest(messageDigest,buffers);
		PoolManager.poolBufferInstance(buffers);
	}

	private boolean isEnd;
	public synchronized void end(){
		isEnd=true;
		notify();
	}
	
	public synchronized void waitForEnd(){
		while(true){
			if(isEnd){
				return;
			}
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
