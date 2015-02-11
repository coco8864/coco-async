package naru.async.store4;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Page {
	private static PageManager pageManager=PageManager.getInstance();
	//read mode ,write mode
	
	private enum Mode{
		READ,//FileÅ@or Buffer 
		WRIET,
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
	
	void init(int length){
		byte[] bytes=new byte[length];
		init(bytes);
	}
	
	void init(byte[] bytes){
		ByteBuffer byteBuffer=ByteBuffer.wrap(bytes);
		init(byteBuffer);
	}
	
	void init(ByteBuffer byteBuffer){
		setBytes(byteBuffer.array());
		ByteBufferLife life=new ByteBufferLife(byteBuffer,this);
		byteBufferLifes.add(life);
		byteBufferPool.put(life, byteBuffer);
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
	
	synchronized ByteBuffer getBuffer(){
	}
	
	synchronized void poolBuffer(ByteBuffer buffer){
	}
	
	synchronized  void gcByteBufferLife(ByteBufferLife byteBufferLife) {
	}
	

}
