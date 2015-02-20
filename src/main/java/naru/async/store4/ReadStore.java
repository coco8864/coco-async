package naru.async.store4;

import java.nio.ByteBuffer;
import java.util.List;

public class ReadStore extends Store {
	private Integer currentPageId;

	@Override
	public boolean putBuffer(ByteBuffer[] buffers) {
		throw new UnsupportedOperationException("ReadStore putBuffer");
	}

	private void buffer(){
		while(true){
			Page page=pageManager.getIdPage(currentPageId);
			currentPageId=page.getNextPageId();
		}
	}
	
	
	@Override
	public boolean nextBuffer() {
		List buffers=null;
		while(true){
			ByteBuffer buf=page.getBuffer(this);
			page=page.nextPage;
			if(buf==null){
				break;
			}
			if(buffers==null){
				buffers=new ArrayList();
			}
			buffers.add(buf);
		}
		return false;
	}

	@Override
	public boolean closeBuffer() {
		// TODO Auto-generated method stub
		return false;
	}

}
