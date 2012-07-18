package naru.async;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
public class BuffersTester {
	private static Logger logger=Logger.getLogger(BuffersTester.class);
	private long seed;
	private Random random=null;
	private Throwable error=null;
	private long length=0;
	private MessageDigest messageDigest;
	
	public BuffersTester(){
		this(System.currentTimeMillis());
	}
	public BuffersTester(long seed){
		this.seed=seed;
		try {
			messageDigest=MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	public long getSeed(){
		return seed;
	}
	
	public long getLength(){
		return length;
	}
	
	public void check() throws Throwable{
		if(error!=null){
			throw error;
		}
	}
	
	public void setError(Throwable error) {
		this.error=error;
	}
	
	private byte[] nextBytes(int len){
		return nextBytes(len,null);
	}
	private byte[] nextBytes(int len,byte[] bytes){
		if(bytes==null){
			bytes=(byte[])PoolManager.getArrayInstance(byte.class, len);
		}
		random.nextBytes(bytes);
		return bytes;
	}

	public ByteBuffer getBuffer(){
		ByteBuffer buffer=PoolManager.getBufferInstance();
		if(random==null){
			random=new Random(seed);
			buffer.putLong(seed);
			byte[] bytes=nextBytes(buffer.remaining());
			buffer.put(bytes);
			PoolManager.poolArrayInstance(bytes);
		}else{
			int len=buffer.remaining();
			nextBytes(len,buffer.array());
			buffer.position(buffer.position()+len);
		}
		length+=buffer.limit();
		buffer.flip();
		messageDigest.update(buffer.array(),buffer.position(),buffer.remaining());
		return buffer;
	}
	
	public ByteBuffer[] getBuffers(){
		return BuffersUtil.toByteBufferArray(getBuffer());
	}
	
	public List<ByteBuffer> getAllBuffers(long length){
		List<ByteBuffer> result=new ArrayList<ByteBuffer>();
		for(long curLength=0;curLength<length;){
			ByteBuffer buffer=getBuffer();
			curLength+=buffer.remaining();
			result.add(buffer);
		}
		return result;
	}
	
	public String getDigest(){
		return DataUtil.digest(messageDigest);
	}
	
	public void putBuffer(ByteBuffer[] buffers){
		for(ByteBuffer buffer:buffers){
			length+=buffer.remaining();
			putBuffer(buffer);
		}
		PoolManager.poolArrayInstance(buffers);
	}
	
	public void putBuffer(ByteBuffer buffer){
		if(random==null){
			seed=buffer.getLong();
			random=new Random(seed);
		}
		int len=buffer.remaining();
		int off=buffer.position();
		byte[] actual=buffer.array();
		byte[] expected=nextBytes(len);
		try{
			for(int i=0;i<len;i++){
				assertEquals(expected[i],actual[off+i]);
			}
		}catch(Throwable t){
			logger.error("fail to check.error length:"+getLength(),t);
			System.out.println("error length:"+getLength());
			error=t;
		}finally{
			PoolManager.poolArrayInstance(expected);
			PoolManager.poolBufferInstance(buffer);
		}
	}
	
}
