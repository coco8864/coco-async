package naru.async.store2;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class PagePool {
	private static PagePool instance=new PagePool();
	public static PagePool getInstance(){
		return instance;
	}

	private Map<Integer,Page> pushPages=new HashMap<Integer,Page>();
	private LinkedList<Page> firstQueue=new LinkedList<Page>();
	private LinkedList<Page> secondQueue=new LinkedList<Page>();
	
	public int pushPage(ByteBuffer[] buffer){
		return 0;
	}
	public ByteBuffer[] popPage(int pageId,boolean callback){
		
		
		return null;
	}
	
	public void removePage(int pageId){
	}

}
