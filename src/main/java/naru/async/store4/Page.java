package naru.async.store4;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import naru.async.BufferGetter;

public class Page {
	private static PageManager pageManager=PageManager.getInstance();
	//read mode ,write mode
	
	private enum Mode{
		READ,//FileÅ@or Buffer 
		WRITE,
	}
	
	//óLå¯Ç»dataÇÃèäç›
	protected enum Location {
		POOL,
		BYTES,
		FILE,
		BOTH
	}
	
	private Mode mode;
	private Location location;
	
	private int pageId;
	private int nextPageId;
	private int length;
	private byte[] bytes;
	private int fileId;
	private long fileOffset;
	private List<ByteBufferLife> byteBufferLifes=new LinkedList<ByteBufferLife>();
	private Map<ByteBufferLife,ByteBuffer> byteBufferPool=new HashMap<ByteBufferLife,ByteBuffer>();
	
	private void setBytes(byte[] bytes){
		pageManager.changeBytesPage(this.bytes,bytes,this);
		this.bytes=bytes;
	}
	
	void setup(int length){
		byte[] bytes=new byte[length];
		setup(bytes);
	}
	
	void setup(byte[] bytes){
		ByteBuffer byteBuffer=ByteBuffer.wrap(bytes);
		setup(byteBuffer);
	}
	
	void setup(ByteBuffer byteBuffer){
		setBytes(byteBuffer.array());
		ByteBufferLife life=new ByteBufferLife(byteBuffer,this);
		byteBufferLifes.add(life);
		byteBufferPool.put(life, byteBuffer);
		mode=Mode.WRITE;
		location=Location.BYTES;
	}
	
	void setup(int fileId,long fileOffset,int length){
		this.fileId=fileId;
		this.fileOffset=fileOffset;
		this.length=length;
		mode=Mode.READ;
		location=Location.FILE;
	}
	
	byte[] getBytes(){
		return bytes;
	}
	
	synchronized void flip(){
	}
	
	synchronized void swapIn(){
		//GET_FILE->GET_MEM;
	}
	
	synchronized void swapOut(){
		//GET_MEM->GET_FILE
	}
	
	synchronized ByteBuffer getBuffer(BufferGetter getter){
		synchronized(byteBufferPool){
			byteBufferPool.remove(key)
		}
	}
	
	synchronized void poolBuffer(ByteBuffer buffer){
	}
	
	synchronized  void gcByteBufferLife(ByteBufferLife byteBufferLife) {
	}
	

}
