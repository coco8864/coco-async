package naru.async.store4;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

public class ByteBufferLife extends WeakReference {
	private static Logger logger=Logger.getLogger(ByteBufferLife.class);
	private static PageManager pageManager=PageManager.getInstance();
	/* ByteBufferをキーにByteBufferLifeを検索する機能が必要 */

	private Page page;
	private int hashCode;
	//for debug
	private Throwable stackOfGet;
	private long timeOfGet;
	private String threadNameOfGet;
	private Throwable stackOfPool;
	private long timeOfPool;
	private String threadNameOfPool;
	
	public ByteBufferLife(ByteBuffer referent,Page page) {
		super(referent,pageManager.getReferenceQueue());
		this.hashCode=pageManager.byteBufferLifeCounter();
		this.page=page;
	}
	
	public byte[] getBytes(){
		return page.getBytes();
	}

	void gcInstance() {
		page.gcByteBufferLife(this);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}
	@Override
	public boolean equals(Object obj) {
		return this==obj;
	}
	
	@Override
	public String toString(){
		return "ByteBufferLife@" +hashCode;
	}
	
}
