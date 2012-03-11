package naru.async.pool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

//import naru.util.DigestUtil;

import org.apache.log4j.Logger;

public class BuffersUtil {
	static private Logger logger=Logger.getLogger(BuffersUtil.class);
	
	public static String toStringFromBuffer(ByteBuffer buffer,String enc){
		byte[] array=buffer.array();
		try {
			return new String(array,buffer.position(),buffer.remaining(),enc);
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}
	
	public static ByteBuffer toBuffer(InputStream is) throws IOException{
		ByteBuffer buffer=PoolManager.getBufferInstance();
		int length=is.read(buffer.array());
		if(length<=0){
			PoolManager.poolBufferInstance(buffer);
			return null;
		}
		buffer.position(length);
		return buffer;
	}
	
	public static void toStream(ByteBuffer buffer,OutputStream os) throws IOException{
		os.write(buffer.array(),buffer.position(),buffer.remaining());
		PoolManager.poolBufferInstance(buffer);
	}

	public static void toStream(ByteBuffer[] buffers,OutputStream os) throws IOException{
		for(int i=0;i<buffers.length;i++){
			toStream(buffers[i],os);
		}
		PoolManager.poolArrayInstance(buffers);
	}
	
	public static void updateDigest(MessageDigest digest,ByteBuffer[] buffers){
		for(int i=0;i<buffers.length;i++){
			ByteBuffer buf=buffers[i];
			int pos=buf.position();
			int len=buf.limit()-pos;
			digest.update(buf.array(),pos,len);
		}
	}
	
	public static ByteBuffer[] newByteBufferArray(int count){
		return (ByteBuffer[])PoolManager.getArrayInstance(ByteBuffer.class, count);
	}
	
	public static ByteBuffer[] toByteBufferArray(ByteBuffer buffer){
		ByteBuffer[] b=newByteBufferArray(1);
		b[0]=buffer;
		return b;
	}
	
	/* length分のbuffersを確保する */
	public static ByteBuffer[] prepareBuffers(long length){
		if(length<=0){
			return (ByteBuffer[])PoolManager.getArrayInstance(ByteBuffer.class, 0);
		}
//		List<ByteBuffer> bufList=new ArrayList<ByteBuffer>();
		int bufferSize=PoolManager.getDefaultBufferSize();
		int bufferCount=(int)(((length-1)/(long)bufferSize)+1);
		ByteBuffer[] result=(ByteBuffer[])PoolManager.getArrayInstance(ByteBuffer.class, bufferCount);
		long lenSum=0;
		for(int i=0;i<bufferCount;i++){
			ByteBuffer buf=PoolManager.getBufferInstance();
			result[i]=buf;
			int capacity=buf.capacity();
			lenSum+=(long)capacity;
			if(lenSum>=length){
				buf.limit(capacity-(int)(lenSum-length));
				break;
			}
		}
		return result;
//		ByteBuffer[] b=newByteBufferArray(bufList.size());こんな心配はなかった
//		ByteBuffer[] c=(ByteBuffer[])bufList.toArray(b);
//		if(b!=c){
//			logger.warn("prepareBuffers not equal array");
//		}
//		return c;
	}

	public static ByteBuffer[] dupBuffers(ByteBuffer[] buffers){
		ArrayList<ByteBuffer> list=new ArrayList<ByteBuffer>();
		for(int i=0;i<buffers.length;i++){
			if(!buffers[i].hasRemaining()){
				continue;//中身のないbufferをdupする必要はない
			}
			ByteBuffer buf=PoolManager.getBufferInstance(buffers[i].capacity());
			buffers[i].mark();
			buf.put(buffers[i]);
			buffers[i].reset();
			buf.flip();
			list.add(buf);
		}
		return (ByteBuffer[])list.toArray(newByteBufferArray(list.size()));
	}
	
	public static ByteBuffer[] buffers(byte[] src, int offset, int length){
		ArrayList list=new ArrayList();
		int position=offset;
		int leftLength=length;
		while(leftLength>0){
			ByteBuffer buf=PoolManager.getBufferInstance();
			int len=leftLength;
			if(len>buf.capacity()){
				len=buf.capacity();
			}
			buf.put(src,position,len);
			buf.flip();
			leftLength-=len;
			position+=len;
			list.add(buf);
		}
		return (ByteBuffer[])list.toArray(newByteBufferArray(list.size()));
	}

	public static long compactBuffers(ByteBuffer[] buffers){
		long total =0;
		for(int i=0;i<buffers.length;i++){
			buffers[i].compact();
			buffers[i].flip();
		}
		return total;
	}

	public static void rewindBuffers(ByteBuffer[] buffers){
		for(int i=0;i<buffers.length;i++){
			buffers[i].rewind();
			buffers[i].compact();
			buffers[i].flip();
		}
	}
	
	public static ByteBuffer[] flipBuffers(ByteBuffer[] buffers){
		for(int i=0;i<buffers.length;i++){
			buffers[i].flip();
		}
		return buffers;
	}
	

	public static long lengthOfBuffers(ByteBuffer[] buffers){
		long total =0;
		for(int i=0;i<buffers.length;i++){
			total+=buffers[i].limit();
		}
		return total;
	}

	public static long remaining(ByteBuffer[] buffers){
		if(buffers==null){
			return 0;
		}
		long total =0;
		for(int i=0;i<buffers.length;i++){
			total+=buffers[i].remaining();
		}
		return total;
	}
	
	public static boolean hasRemaining(ByteBuffer[] buffers){
		if(buffers==null){
			return false;
		}
		for(int i=0;i<buffers.length;i++){
			if( buffers[i].hasRemaining() ){
				return true;
			}
		}
		return false;
	}
	

	public static void mark(ByteBuffer[] buffers){
		for(int i=0;i<buffers.length;i++){
			buffers[i].mark();
		}
	}

	public static void reset(ByteBuffer[] buffers){
		for(int i=0;i<buffers.length;i++){
			buffers[i].reset();
		}
	}
	
	/**
	 * 先頭を削る
	 * @param buffers
	 * @param length
	 */
	public static void skip(ByteBuffer[] buffers,long length){
		for(int i=0;i<buffers.length;i++){
			ByteBuffer buf=buffers[i];
			long remaining=(long)buf.remaining();
			if(length>=remaining){
				buf.position(buf.limit());
				length-=remaining;
			}else{
				int position=buf.position();
				position+=(int)(length);
				buf.position(position);
				return;
			}
		}
	}
	
	public static void cut(ByteBuffer buffer,long length){
		long remaining=(long)buffer.remaining();
		if(length<remaining){
			int position=buffer.position();
			buffer.limit(position+(int)length);
		}
	}
	
	/**
	 * お尻を削る
	 * @param buffers
	 * @param length
	 */
	public static void cut(ByteBuffer[] buffers,long length){
		for(int i=0;i<buffers.length;i++){
			ByteBuffer buf=buffers[i];
			long remaining=(long)buf.remaining();
			if(length>=remaining){
				length-=remaining;
			}else{
				int position=buf.position();
				buf.limit(position+(int)length);
				length=0;
			}
		}
	}
	
	/**
	 * 中間を切り取る
	 */
	public static void slice(ByteBuffer[] buffers,long offset,long length){
		int i=0;
		for(;i<buffers.length;i++){
			ByteBuffer buf=buffers[i];
			long remaining=(long)buf.remaining();
			if(offset>=remaining){
				buf.position(buf.limit());
				offset-=remaining;
			}else{
				int position=buf.position();
				position+=(int)(offset);
				buf.position(position);
				break;
			}
		}
		for(;i<buffers.length;i++){
			ByteBuffer buf=buffers[i];
			long remaining=(long)buf.remaining();
			if(length>=remaining){
				length-=remaining;
			}else{
				int position=buf.position();
				buf.limit(position+(int)length);
				length=0;
			}
		}
	}
	
	public static ByteBuffer[] concatenate(ByteBuffer[] part1,ByteBuffer[] part2){
		if(part1==null){
			return part2;
		}
		if(part2==null){
			return part1;
		}
		int length=part1.length+part2.length;
		ByteBuffer[] result=newByteBufferArray(length);
		int i=0;
		for(int j=0;j<part1.length;j++){
			result[i]=part1[j];
			i++;
		}
		PoolManager.poolArrayInstance(part1);
		for(int j=0;j<part2.length;j++){
			result[i]=part2[j];
			i++;
		}
		PoolManager.poolArrayInstance(part2);
		return result;
	}

	public static ByteBuffer[] concatenate(ByteBuffer[] part1,ByteBuffer buffer,ByteBuffer[] part2){
		int length=part1.length+part2.length+1;
		ByteBuffer[] result=newByteBufferArray(length);
		int i=0;
		for(int j=0;j<part1.length;j++){
			result[i]=part1[j];
			i++;
		}
		PoolManager.poolArrayInstance(part1);
		result[i]=buffer;
		i++;
		for(int j=0;j<part2.length;j++){
			result[i]=part2[j];
			i++;
		}
		PoolManager.poolArrayInstance(part2);
		return result;
	}
	
	
	public static ByteBuffer[] concatenate(ByteBuffer head,ByteBuffer[] contents,ByteBuffer tail){
		int length=0;
		if(head!=null){
			length++;
		}
		if(contents!=null){
			length+=contents.length;
		}
		if(tail!=null){
			length++;
		}
		ByteBuffer[] result=newByteBufferArray(length);
		int i=0;
		if(head!=null){
			result[i]=head;
			i++;
		}
		if(contents!=null){
			for(int j=0;j<contents.length;j++){
				result[i]=contents[j];
				i++;
			}
			PoolManager.poolArrayInstance(contents);
		}
		if(tail!=null){
			result[i]=tail;
			i++;
		}
		return result;
	}
	
	public static void peekBuffer(ByteBuffer[] buffers){
		try {
			File file=File.createTempFile("peekbuffer", ".dmp",new File("."));
			RandomAccessFile raf=new RandomAccessFile(file,"rwd");
			FileChannel writeChannel=raf.getChannel();
			mark(buffers);
			writeChannel.write(buffers);
			reset(buffers);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
