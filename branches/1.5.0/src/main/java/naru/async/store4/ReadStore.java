package naru.async.store4;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ReadStore extends Store implements PageCallback {
	private Integer currentPageId;
	private boolean processing=false;

	@Override
	public boolean putBuffer(ByteBuffer[] buffers) {
		throw new UnsupportedOperationException("ReadStore putBuffer");
	}

	private boolean collectBuffer(ByteBuffer buffer){
		List<ByteBuffer> buffers=null;
		if(buffer!=null){
			buffers=new ArrayList<ByteBuffer>();
			buffers.add(buffer);
		}
		boolean waiting=false;
		while(currentPageId>0){
			Page page=pageManager.getIdPage(currentPageId);
			currentPageId=page.getNextPageId();
			buffer=page.getBuffer(this);
			if(buffer==null){
				waiting=true;
				break;
			}
			if(buffers==null){
				buffers=new ArrayList<ByteBuffer>();
			}
			buffers.add(buffer);
		}
		boolean isCallback=false;
		if(buffers!=null){
			queueBufferCallback(buffers);
			isCallback=true;
		}
		if(waiting==false){
			queueEndCallback();
			isCallback=true;
		}
		return isCallback;
	}
	
	/**
	 * @return Ç±ÇÃèàóùÇ≈callbackÇ∑ÇÍÇŒtrue
	 */
	@Override
	public boolean nextBuffer() {
		synchronized(this){
			if(processing){
				return false;
			}
			processing=true;
		}
		if(collectBuffer(null)){
			processing=false;
			return true;
		}
		return false;
	}

	@Override
	public boolean closeBuffer() {
		// TODO Auto-generated method stub
		return false;
	}

	public void onBuffer(ByteBuffer buffer) {
		collectBuffer(buffer);
		processing=false;
	}

	public void onBufferFailure(Throwable failure) {
		queueFailureCallback(failure);
		processing=false;
	}

}
