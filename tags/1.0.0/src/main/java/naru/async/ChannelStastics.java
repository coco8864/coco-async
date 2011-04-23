package naru.async;

public class ChannelStastics {
	private int asyncConnectCount;
	private int asyncAcceptCount;
	private int asyncReadCount;
	private int asyncReadFailCount;
	private int asyncWriteCount;
	private int asyncWriteFailCount;
	private long asyncWriteLength;
	private long writeLength;
	private int asyncCloseCount;
	private int asyncCloseFailCount;
	private int asyncCancelCount;
	private int asyncCancelFailCount;
	
	//正常系メソッド群
	private int onAcceptableCount;
	private int onAcceptedCount;
	private int onConnectedCount;
	private int onReadCount;
	private long onReadLength;
	private int onWrittenCount;
	
	//onTimeoutメソッド群
	private int onAcceptFailureCount;
	private int onConnectFailureCount;
	private int onWriteFailureCount;
	private int onReadFailureCount;
	private int onCloseFailureCount;
	private int onCancelFailureCount;
	
	//onTimeoutメソッド群
	private int onConnectTimeoutCount;
	private int onWriteTimeoutCount;
	private int onReadTimeoutCount;
	
	//onClosedメソッド群
	private int onAcceptClosedCount;
	private int onConnectClosedCount;
	private int onWriteClosedCount;
	private int onReadClosedCount;
	private int onCloseClosedCount;
	private int onCancelClosedCount;
	
	private int onFinishedCount;
	
	public void recycle(){
		onAcceptableCount=asyncConnectCount=asyncAcceptCount=asyncReadCount=asyncReadFailCount=asyncWriteCount
		=asyncWriteFailCount=asyncCloseCount=asyncCloseFailCount=asyncCancelCount=asyncCancelFailCount
		=onAcceptedCount=onConnectedCount=onReadCount=onWrittenCount
		=onAcceptFailureCount=onConnectFailureCount=onWriteFailureCount=onReadFailureCount=onCloseFailureCount=onCancelFailureCount
		=onConnectTimeoutCount=onWriteTimeoutCount=onReadTimeoutCount
		=onAcceptClosedCount=onConnectClosedCount=onWriteClosedCount=onReadClosedCount=onCloseClosedCount=onCancelClosedCount
		=onFinishedCount
		=0;
		asyncWriteLength=onReadLength=writeLength=0L;
	}
	
	public ChannelStastics sum(ChannelStastics s){
		asyncConnectCount+=s.asyncConnectCount;
		asyncAcceptCount+=s.asyncAcceptCount;
		asyncReadCount+=s.asyncReadCount;
		asyncReadFailCount+=s.asyncReadFailCount;
		asyncWriteCount+=s.asyncWriteCount;
		asyncWriteFailCount+=s.asyncWriteFailCount;
		asyncCloseCount+=s.asyncCloseCount;
		asyncCloseFailCount+=s.asyncCloseFailCount;
		asyncCancelCount+=s.asyncCancelCount;
		asyncCancelFailCount+=s.asyncCancelFailCount;
		
		onAcceptableCount+=s.onAcceptableCount;
		onAcceptedCount+=s.onAcceptedCount;
		onConnectedCount+=s.onConnectedCount;
		onReadCount+=s.onReadCount;
		onWrittenCount+=s.onWrittenCount;
		
		onAcceptFailureCount+=s.onAcceptFailureCount;
		onConnectFailureCount+=s.onConnectFailureCount;
		onWriteFailureCount+=s.onWriteFailureCount;
		onReadFailureCount+=s.onReadFailureCount;
		onCloseFailureCount+=s.onCloseFailureCount;
		onCancelFailureCount+=s.onCancelFailureCount;
		onConnectTimeoutCount+=s.onConnectTimeoutCount;
		onWriteTimeoutCount+=s.onWriteTimeoutCount;
		onReadTimeoutCount+=s.onReadTimeoutCount;
		onAcceptClosedCount+=s.onAcceptClosedCount;
		onConnectClosedCount+=s.onConnectClosedCount;
		onWriteClosedCount+=s.onWriteClosedCount;
		onReadClosedCount+=s.onReadClosedCount;
		onCloseClosedCount+=s.onCloseClosedCount;
		onCancelClosedCount+=s.onCancelClosedCount;
		onFinishedCount+=s.onFinishedCount;
		
		asyncWriteLength+=s.asyncWriteLength;
		writeLength+=s.writeLength;
		onReadLength+=s.onReadLength;
		return this;
	}

	public int getAsyncConnectCount() {
		return asyncConnectCount;
	}

	public int getAsyncAcceptCount() {
		return asyncAcceptCount;
	}

	public int getAsyncReadCount() {
		return asyncReadCount;
	}

	public int getAsyncReadFailCount() {
		return asyncReadFailCount;
	}

	public int getAsyncWriteCount() {
		return asyncWriteCount;
	}

	public int getAsyncWriteFailCount() {
		return asyncWriteFailCount;
	}

	public long getAsyncWriteLength() {
		return asyncWriteLength;
	}

	public long getWriteLength() {
		return writeLength;
	}

	public int getAsyncCloseCount() {
		return asyncCloseCount;
	}

	public int getAsyncCloseFailCount() {
		return asyncCloseFailCount;
	}

	public int getAsyncCancelCount() {
		return asyncCancelCount;
	}

	public int getAsyncCancelFailCount() {
		return asyncCancelFailCount;
	}

	public int getOnAcceptedCount() {
		return onAcceptedCount;
	}
	public int getOnAcceptableCount(){
		return onAcceptableCount;
	}
	public int getOnConnectedCount() {
		return onConnectedCount;
	}

	public int getOnReadCount() {
		return onReadCount;
	}

	public long getOnReadLength() {
		return onReadLength;
	}

	public int getOnWrittenCount() {
		return onWrittenCount;
	}

	public int getOnAcceptFailureCount() {
		return onAcceptFailureCount;
	}

	public int getOnConnectFailureCount() {
		return onConnectFailureCount;
	}

	public int getOnWriteFailureCount() {
		return onWriteFailureCount;
	}

	public int getOnReadFailureCount() {
		return onReadFailureCount;
	}

	public int getOnCloseFailureCount() {
		return onCloseFailureCount;
	}

	public int getOnCancelFailureCount() {
		return onCancelFailureCount;
	}

	public int getOnConnectTimeoutCount() {
		return onConnectTimeoutCount;
	}

	public int getOnWriteTimeoutCount() {
		return onWriteTimeoutCount;
	}

	public int getOnReadTimeoutCount() {
		return onReadTimeoutCount;
	}

	public int getOnAcceptClosedCount() {
		return onAcceptClosedCount;
	}

	public int getOnConnectClosedCount() {
		return onConnectClosedCount;
	}

	public int getOnWriteClosedCount() {
		return onWriteClosedCount;
	}

	public int getOnReadClosedCount() {
		return onReadClosedCount;
	}

	public int getOnCloseClosedCount() {
		return onCloseClosedCount;
	}

	public int getOnCancelClosedCount() {
		return onCancelClosedCount;
	}

	public int getOnFinishedCount() {
		return onFinishedCount;
	}

	public void asyncConnect() {
		this.asyncConnectCount++;
	}

	public void asyncAccept() {
		this.asyncAcceptCount++;
	}

	public void asyncRead() {
		asyncReadCount++;
	}

	public void asyncReadFail() {
		this.asyncReadFailCount++;
	}

	public void asyncWrite() {
		this.asyncWriteCount++;
	}

	public void asyncWriteFail() {
		this.asyncWriteFailCount++;
	}

	public long addAsyncWriteLength(long asyncWriteLength) {
		asyncWrite();
		this.asyncWriteLength+= asyncWriteLength;
		return this.asyncWriteLength;
	}

	public long addWriteLength(long writeLength) {
		this.writeLength+= writeLength;
		return this.writeLength;
	}

	public void asyncClose() {
		this.asyncCloseCount++;
	}

	public void asyncCloseFail() {
		this.asyncCloseFailCount++;
	}

	public void asyncCancel() {
		this.asyncCancelCount++;
	}

	public void asyncCancelFail() {
		this.asyncCancelFailCount++;
	}
	
	public void onAcceptable(){
		onAcceptableCount++;
	}

	public void onAccepted() {
		this.onAcceptedCount++;
	}

	public void onConnected() {
		this.onConnectedCount++;
	}

	public void onRead() {
		this.onReadCount++;
	}

	public long addOnReadLength(long onReadLength) {
		this.onReadLength+= onReadLength;
		return this.onReadLength;
	}

	public void onWritten() {
		this.onWrittenCount++;
	}

	public void onAcceptFailure() {
		this.onAcceptFailureCount++;
	}

	public void onConnectFailure() {
		this.onConnectFailureCount++;
	}

	public void onWriteFailure() {
		this.onWriteFailureCount++;
	}

	public void onReadFailure() {
		this.onReadFailureCount++;
	}

	public void onCloseFailure() {
		this.onCloseFailureCount++;
	}

	public void onCancelFailure() {
		this.onCancelFailureCount++;
	}

	public void onConnectTimeout() {
		this.onConnectTimeoutCount++;
	}

	public void onWriteTimeout() {
		this.onWriteTimeoutCount++;
	}

	public void onReadTimeout() {
		this.onReadTimeoutCount++;
	}

	public void onAcceptClosed() {
		this.onAcceptClosedCount++;
	}

	public void onConnectClosed() {
		this.onConnectClosedCount++;
	}

	public void onWriteClosed() {
		this.onWriteClosedCount++;
	}

	public void onReadClosed() {
		this.onReadClosedCount++;
	}

	public void onCloseClosed() {
		this.onCloseClosedCount++;
	}

	public void onCancelClosed() {
		this.onCancelClosedCount++;
	}

	public void onFinished() {
		this.onFinishedCount++;
	}
	
}
