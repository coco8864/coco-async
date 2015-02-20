package naru.async.store4;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Page {
	private static PageManager pageManager=PageManager.getInstance();
	private static FileManager fileManager=FileManager.getInstance();
	//read mode ,write mode
	private enum Stat{
		POOL_WITH_BUFFER,
		POOL,
		WRITE,
		READ,//Buffer 
		PAGING_OUT,//書き込み中
		PAGEOUT,
		PAGING_IN//読み込み中
	}
	
	private enum Persistence{
		PERSISTENT,//永続Page
		VOLATILE//揮発Page
	}
	
	private Stat stat;//どのqueueに属しているか？
	private Persistence persistence;
	
	private Integer pageId;//*
	public Integer getPageId() {
		return pageId;
	}

	private Integer nextPageId;//*
	public Integer getNextPageId() {
		return nextPageId;
	}

	private int length;//*
	private int fileId;//*
	private long fileOffset;//*
	
	private byte[] bytes;
	private List<ByteBufferLife> byteBufferLifes=new LinkedList<ByteBufferLife>();
	private Map<ByteBufferLife,ByteBuffer> byteBufferPool=new HashMap<ByteBufferLife,ByteBuffer>();
	
	ByteBufferLife getByteBufferLife(ByteBuffer buffer){
		synchronized(byteBufferLifes){
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			while(itr.hasNext()){
				ByteBufferLife life=itr.next();
				if(life.get()==buffer){
					return life;
				}
			}
		}
		return null;
	}
	private void putByteBufferLife(ByteBufferLife life){
		synchronized(byteBufferLifes){
			byteBufferLifes.add(life);
		}
	}
	
	private int liveBufferCount(){
		return (byteBufferLifes.size()-byteBufferPool.size());
	}
	
	private void set
	
	private List<PageCallback> callbacks=new ArrayList<PageCallback>();
	
	
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
		putByteBufferLife(life);
		byteBufferPool.put(life, byteBuffer);
		stat=Stat.WRITE;
		persistence=Persistence.VOLATILE;
	}
	
	void setup(int fileId,long fileOffset,int length){
		this.fileId=fileId;
		this.fileOffset=fileOffset;
		this.length=length;
		stat=Stat.PAGEOUT;
		persistence=Persistence.PERSISTENT;
	}
	
	byte[] getBytes(){
		return bytes;
	}
	
	synchronized void flip(int length){
		//WRITE->READ
		stat=Stat.READ;
		this.length=length;
	}
	
	void pageIn(){
		//PAGEOUT->
		Page tmpPage=pageManager.getBufferFreePage(length);
		synchronized(this){
			stat=stat.PAGING_IN;
		}
		fileManager.read(this);
		synchronized(this){
			stat=stat.READ;
		}
		while(true){
			
		}
	}
	
	void pageOut(){
		//READ->
		synchronized(this){
			stat=stat.PAGING_OUT;
		}
		fileManager.write(this);
		synchronized(this){
			stat=stat.PAGEOUT;
		}
	}
	
	synchronized ByteBuffer getBuffer(PageCallback callback){
		synchronized(byteBufferPool){
			Iterator<ByteBufferLife> itr=byteBufferPool.keySet().iterator();
			ByteBuffer buffer=null;
			if(itr.hasNext()){
				ByteBufferLife life=itr.next();
				buffer=(ByteBuffer)life.get();
				itr.remove();
				return buffer;
			}
		}
		if(bytes==null){
			callbacks.add(callback);
			return null;//swapinさせるのは呼び出し元の責務
		}
		ByteBuffer buffer=ByteBuffer.wrap(bytes);
		ByteBufferLife life=new ByteBufferLife(buffer,this);
		putByteBufferLife(life);
		buffer.position(0).limit(length);
		return buffer;
	}
	
	synchronized void poolBuffer(ByteBuffer buffer){
		ByteBufferLife life=getByteBufferLife(buffer);
		if(life==null){
			return;
		}
		byteBufferPool.put(life,buffer);
		if(byteBufferPool.size()==byteBufferLifes.size()&&fileId==0){
			//このページは未使用、再利用可
		}
	}
	
	synchronized  void gcByteBufferLife(ByteBufferLife byteBufferLife) {
	}
	

}
