package naru.async.core;

public class SelectorStastics {
	private int id;
	private long interval;
	
	private long loopCount;
	private long sleepCount;
	private long acceptRefuseCount;
	/**
	 * èdï°ÇµÇƒqueueÇ≥ÇÍÇÈÇΩÇﬂÅAselectoríÜÇÃcontextêîÇ∆ÇÕÉäÉìÉNÇµÇ»Ç¢
	 */
	private long inQueueCount;
	/**
	 * åªç›selectoríÜÇ…ë∂ç›Ç∑ÇÈcontextÇÃÇ©Ç∏
	 */
	private int selectCount;
	private long writeCount;
	private long readCount;
	private long connectCount;
	
	SelectorStastics(int id,long selectInterval){
		this.id=id;
		this.interval=selectInterval;
	}

	public int getId() {
		return id;
	}
	
	public long getInterval() {
		return interval;
	}

	public void loop(){
		loopCount++;
	}
	public long getLoopCount() {
		return loopCount;
	}

	public void sleep(){
		sleepCount++;
	}
	public long getSleepCount() {
		return sleepCount;
	}


	public void acceptRefuse(){
		acceptRefuseCount++;
	}
	public long getAcceptRefuseCount() {
		return acceptRefuseCount;
	}

	public void inQueue(){
		inQueueCount++;
	}
	public long getInQueueCount() {
		return inQueueCount;
	}

	public void write(){
		writeCount++;
	}
	public long getWriteCount() {
		return writeCount;
	}

	public void read(){
		readCount++;
	}
	public long getReadCount() {
		return readCount;
	}
	
	public void connect(){
		connectCount++;
	}
	public long getConnectCount() {
		return connectCount;
	}

	public void setSelectCount(int selectCount){
		this.selectCount=selectCount;
	}
	public long getSelectCount() {
		return selectCount;
	}
	
}
