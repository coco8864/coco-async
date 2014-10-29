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

import naru.async.Log;

import org.apache.log4j.Logger;

public class BuffersUtil {
	private static Logger logger = Logger.getLogger(BuffersUtil.class);

	public static void addByteBufferList(List<ByteBuffer> list,ByteBuffer[] buffers){
		for(ByteBuffer buffer:buffers){
			list.add(buffer);
		}
		PoolManager.poolArrayInstance(buffers);
	}
	
	public static boolean copyBuffers(List<ByteBuffer> srcBuffers,ByteBuffer dstBuffer){
		boolean rc=false;
		for(ByteBuffer srcBuffer:srcBuffers){
			if(!srcBuffer.hasRemaining()){
				continue;
			}
			dstBuffer.put(srcBuffer);
			if(dstBuffer.hasRemaining()){
				rc=true;
				break;
			}
		}
		dstBuffer.flip();
		return rc;
	}
	
	public static boolean copyBuffers(List<ByteBuffer> srcBuffers,List<ByteBuffer> dstBuffers){
		for(ByteBuffer dstBuffer:dstBuffers){
			if(copyBuffers(srcBuffers,dstBuffer)){
				continue;
			}
			return false;
		}
		return true;
	}

	public static String toStringFromBuffer(ByteBuffer buffer, String enc) {
		byte[] array = buffer.array();
		try {
			return new String(array, buffer.position(), buffer.remaining(), enc);
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

	public static ByteBuffer toBuffer(InputStream is) throws IOException {
		ByteBuffer buffer = PoolManager.getBufferInstance();
		int length = is.read(buffer.array());
		if (length <= 0) {
			PoolManager.poolBufferInstance(buffer);
			return null;
		}
		buffer.position(length);
		return buffer;
	}

	public static void toStream(ByteBuffer buffer, OutputStream os)
			throws IOException {
		os.write(buffer.array(), buffer.position(), buffer.remaining());
		PoolManager.poolBufferInstance(buffer);
	}

	public static void toStream(ByteBuffer[] buffers, OutputStream os)
			throws IOException {
		for (int i = 0; i < buffers.length; i++) {
			toStream(buffers[i], os);
		}
		PoolManager.poolArrayInstance(buffers);
	}

	public static void updateDigest(MessageDigest digest, ByteBuffer[] buffers) {
		for (int i = 0; i < buffers.length; i++) {
			ByteBuffer buf = buffers[i];
			int pos = buf.position();
			int len = buf.limit() - pos;
			digest.update(buf.array(), pos, len);
		}
	}

	public static ByteBuffer[] newByteBufferArray(int count) {
		return (ByteBuffer[]) PoolManager.getArrayInstance(ByteBuffer.class,count);
	}

	public static ByteBuffer[] toByteBufferArray(ByteBuffer buffer) {
		ByteBuffer[] b = newByteBufferArray(1);
		b[0] = buffer;
		return b;
	}

	public static ByteBuffer[] toByteBufferArray(List<ByteBuffer> buffers) {
		int size = buffers.size();
		ByteBuffer[] b = newByteBufferArray(size);
		for (int i = 0; i < size; i++) {
			b[i] = buffers.get(i);
		}
		return b;
	}

	/* length byte分のbuffersを確保する */
	public static ByteBuffer[] prepareBuffers(long length) {
		if (length <= 0) {
			return (ByteBuffer[]) PoolManager.getArrayInstance(
					ByteBuffer.class, 0);
		}
		// List<ByteBuffer> bufList=new ArrayList<ByteBuffer>();
		int bufferSize = PoolManager.getDefaultBufferSize();
		int bufferCount = (int) (((length - 1) / (long) bufferSize) + 1);
		ByteBuffer[] result = newByteBufferArray(bufferCount);
		long lenSum = 0;
		for (int i = 0; i < bufferCount; i++) {
			ByteBuffer buf = PoolManager.getBufferInstance();
			result[i] = buf;
			int capacity = buf.capacity();
			lenSum += (long) capacity;
			if (lenSum >= length) {
				buf.limit(capacity - (int) (lenSum - length));
				break;
			}
		}
		return result;
		// ByteBuffer[] b=newByteBufferArray(bufList.size());こんな心配はなかった
		// ByteBuffer[] c=(ByteBuffer[])bufList.toArray(b);
		// if(b!=c){
		// logger.warn("prepareBuffers not equal array");
		// }
		// return c;
	}

	public static ByteBuffer[] dupBuffers(ByteBuffer[] buffers) {
		ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
		for (int i = 0; i < buffers.length; i++) {
			if (!buffers[i].hasRemaining()) {
				continue;// 中身のないbufferをdupする必要はない
			}
			ByteBuffer buf = PoolManager.getBufferInstance(buffers[i].capacity());
			buffers[i].mark();
			buf.put(buffers[i]);
			buffers[i].reset();
			buf.flip();
			list.add(buf);
		}
		return (ByteBuffer[]) list.toArray(newByteBufferArray(list.size()));
	}

	public static ByteBuffer[] buffers(byte[] src, int offset, int length) {
		ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
		int position = offset;
		int leftLength = length;
		while (leftLength > 0) {
			ByteBuffer buf = PoolManager.getBufferInstance();
			int len = leftLength;
			if (len > buf.capacity()) {
				len = buf.capacity();
			}
			buf.put(src, position, len);
			buf.flip();
			leftLength -= len;
			position += len;
			list.add(buf);
		}
		return toByteBufferArray(list);
	}

	public static long compactBuffers(ByteBuffer[] buffers) {
		long total = 0;
		for (int i = 0; i < buffers.length; i++) {
			buffers[i].compact();
			buffers[i].flip();
		}
		return total;
	}

	public static void rewindBuffers(ByteBuffer[] buffers) {
		for (int i = 0; i < buffers.length; i++) {
			buffers[i].rewind();
			buffers[i].compact();
			buffers[i].flip();
		}
	}

	public static ByteBuffer[] flipBuffers(ByteBuffer[] buffers) {
		for (int i = 0; i < buffers.length; i++) {
			buffers[i].flip();
		}
		return buffers;
	}

	public static long lengthOfBuffers(ByteBuffer[] buffers) {
		long total = 0;
		for (int i = 0; i < buffers.length; i++) {
			total += buffers[i].limit();
		}
		return total;
	}

	public static long remaining(ByteBuffer[] buffers) {
		if (buffers == null) {
			return 0;
		}
		long total = 0;
		for (int i = 0; i < buffers.length; i++) {
			total += buffers[i].remaining();
		}
		return total;
	}

	public static boolean hasRemaining(ByteBuffer[] buffers) {
		if (buffers == null) {
			return false;
		}
		for (int i = 0; i < buffers.length; i++) {
			if (buffers[i].hasRemaining()) {
				return true;
			}
		}
		return false;
	}

	public static void mark(ByteBuffer[] buffers) {
		for (int i = 0; i < buffers.length; i++) {
			buffers[i].mark();
		}
	}

	public static void reset(ByteBuffer[] buffers) {
		for (int i = 0; i < buffers.length; i++) {
			buffers[i].reset();
		}
	}

	public static void skip(ByteBuffer buffers, long offset) {
		long remaining = (long) buffers.remaining();
		if (remaining >= offset) {
			int position = buffers.position();
			position += (int) (offset);
			buffers.position(position);
			return;
		}
		throw new RuntimeException("fail to skip");
	}

	/**
	 * 先頭を削る
	 * 
	 * @param buffers
	 * @param length
	 */
	public static void skip(ByteBuffer[] buffers, long length) {
		for (int i = 0; i < buffers.length; i++) {
			ByteBuffer buf = buffers[i];
			long remaining = (long) buf.remaining();
			if (length >= remaining) {
				buf.position(buf.limit());
				length -= remaining;
			} else {
				int position = buf.position();
				position += (int) (length);
				buf.position(position);
				return;
			}
		}
	}

	public static void cut(ByteBuffer buffer, long length) {
		long remaining = (long) buffer.remaining();
		if (length <= remaining) {
			int position = buffer.position();
			buffer.limit(position + (int) length);
			return;
		}
		throw new RuntimeException("fail to cut");
	}

	/**
	 * お尻を削る
	 * 
	 * @param buffers
	 * @param length
	 */
	public static void cut(ByteBuffer[] buffers, long length) {
		for (int i = 0; i < buffers.length; i++) {
			ByteBuffer buf = buffers[i];
			long remaining = (long) buf.remaining();
			if (length >= remaining) {
				length -= remaining;
			} else {
				int position = buf.position();
				buf.limit(position + (int) length);
				length = 0;
			}
		}
	}

	/**
	 * 既存のbuffersのposition,limitは変化させず、新たなbufferに中間を切り取る
	 */
	public static ByteBuffer[] slice(ByteBuffer[] buffers, long offset,
			long length) {
		int i = 0;
		List<ByteBuffer> result = new ArrayList<ByteBuffer>();
		ByteBuffer workBuf = null;
		for (; i < buffers.length; i++) {
			ByteBuffer buf = buffers[i];
			long remaining = (long) buf.remaining();
			if (offset >= remaining) {
				offset -= remaining;
			} else {
				int position = buf.position();
				position += (int) (offset);
				workBuf = PoolManager.duplicateBuffer(buf);
				workBuf.position(position);
				result.add(workBuf);
				break;
			}
		}
		if (workBuf != null) {
			long remaining = (long) workBuf.remaining();
			if (length > remaining) {
				length -= remaining;
			} else {
				int position = workBuf.position();
				workBuf.limit(position + (int) length);
				return toByteBufferArray(result);
				// return (ByteBuffer[])result.toArray(new
				// ByteBuffer[result.size()]);
			}
			i++;
		}
		for (; i < buffers.length; i++) {
			ByteBuffer buf = buffers[i];
			long remaining = (long) buf.remaining();
			if (length > remaining) {
				length -= remaining;
				workBuf = PoolManager.duplicateBuffer(buf);
				result.add(workBuf);
			} else {
				int position = buf.position();
				workBuf = PoolManager.duplicateBuffer(buf);
				workBuf.limit(position + (int) length);
				result.add(workBuf);
				break;
			}
		}
		return toByteBufferArray(result);
		// return (ByteBuffer[])result.toArray(new ByteBuffer[result.size()]);
	}

	public static ByteBuffer[] concatenate(ByteBuffer[] part1,
			ByteBuffer[] part2) {
		if (part1 == null) {
			return part2;
		}
		if (part2 == null) {
			return part1;
		}
		int length = part1.length + part2.length;
		int i = 0;
		ByteBuffer[] result = newByteBufferArray(length);
		System.arraycopy(part1, 0, result, i, part1.length);
		i += part1.length;
		PoolManager.poolArrayInstance(part1);
		System.arraycopy(part2, 0, result, i, part2.length);
		PoolManager.poolArrayInstance(part2);
		return result;
	}

	public static ByteBuffer[] concatenate(ByteBuffer[] part1,
			ByteBuffer buffer, ByteBuffer[] part2) {
		int length = 0;
		if (part1 != null) {
			length += part1.length;
		}
		if (buffer != null) {
			length++;
		}
		if (part2 != null) {
			length += part2.length;
		}
		ByteBuffer[] result = newByteBufferArray(length);
		int i = 0;
		if (part1 != null) {
			System.arraycopy(part1, 0, result, i, part1.length);
			i += part1.length;
			PoolManager.poolArrayInstance(part1);
		}
		if (buffer != null) {
			result[i] = buffer;
			i++;
		}
		if (part2 != null) {
			System.arraycopy(part2, 0, result, i, part2.length);
			PoolManager.poolArrayInstance(part2);
		}
		return result;
	}

	public static ByteBuffer[] concatenate(ByteBuffer head,
			ByteBuffer[] contents, ByteBuffer tail) {
		int length = 0;
		if (head != null) {
			length++;
		}
		if (contents != null) {
			length += contents.length;
		}
		if (tail != null) {
			length++;
		}
		ByteBuffer[] result = newByteBufferArray(length);
		int i = 0;
		if (head != null) {
			result[i] = head;
			i++;
		}
		if (contents != null) {
			System.arraycopy(contents, 0, result, i, contents.length);
			PoolManager.poolArrayInstance(contents);
			i += contents.length;
		}
		if (tail != null) {
			result[i] = tail;
			i++;
		}
		return result;
	}

	public static void peekBuffer(ByteBuffer[] buffers) {
		try {
			File file = File
					.createTempFile("peekbuffer", ".dmp", new File("."));
			RandomAccessFile raf = new RandomAccessFile(file, "rwd");
			FileChannel writeChannel = raf.getChannel();
			mark(buffers);
			writeChannel.write(buffers);
			reset(buffers);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void hexDump(String title,ByteBuffer[] buffers) {
		if(!logger.isDebugEnabled()){
			return;
		}
		for(ByteBuffer buffer:buffers){
			hexDump(title,buffer);
		}
	}

	public static void hexDump(String title,ByteBuffer buffer){
		hexDump(title,buffer.array(),buffer.position(),buffer.remaining());
	}
	
	public static void hexDump(String title,byte[] data){
		hexDump(title,data,0,data.length);
	}
	
	public static synchronized void hexDump(String title,byte[] data, int pos, int length){
		if(!logger.isDebugEnabled()){
			return;
		}
		if ((pos < 0) || ((pos+length) > data.length)) {
			logger.error("hexDump error." +data.length +":"+pos +":" +length);
			return;
		}
		Log.debug(logger,title);
		long base=0;
		StringBuffer buffer = new StringBuffer(74);
		for (int i = pos; i < (pos+length); i += 16) {
			int chars_read = (pos+length) - i;
			if (chars_read > 16) {
				chars_read = 16;
			}
			buffer.append(dump(base)).append(' ');
			for (int j = 0; j < 16; j++) {
				if (j < chars_read) {
					buffer.append(dump(data[j + i]));
				} else {
					buffer.append("  ");
				}
				buffer.append(' ');
			}
			for (int j = 0; j < chars_read; j++) {
				if ((data[j + i] >= ' ') && (data[j + i] < 127)) {
					buffer.append((char) data[j + i]);
				} else {
					buffer.append('.');
				}
			}
			Log.debug(logger,buffer.toString());
			buffer.setLength(0);
			base += chars_read;
		}
	}

	private static final char[] _hexcodes = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
	private static final int[] shifts = { 28, 24, 20, 16, 12, 8, 4, 0 };
	private static StringBuilder lbuffer=new StringBuilder(8);
	private static StringBuilder cbuffer=new StringBuilder(2);
	private static StringBuilder dump(long value) {
		lbuffer.setLength(0);
		for (int i = 0; i < 8; i++) {
			lbuffer.append(_hexcodes[((int) (value >> shifts[i])) & 15]);
		}
		return lbuffer;
	}
	private static StringBuilder dump(byte value) {
		cbuffer.setLength(0);
		for (int i = 0; i < 2; i++) {
			cbuffer.append(_hexcodes[(value >> shifts[i + 6]) & 15]);
		}
		return cbuffer;
	}
}
