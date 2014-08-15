package naru.async.core;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import naru.async.BuffersTester;
import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;

public abstract class TestHandler extends ChannelHandler {
	private static Logger logger=Logger.getLogger(TestHandler.class);
	
	protected BuffersTester tester;
	private CoreTester coreTester;
	private String name;
	private int readCount;
	private int writtenCount;
	private int closedCount;
	private int timeoutCount;
	private int failureCount;
	private int connectFailureCount;
	
	public void sendBuffer(){
		asyncWrite("sendBuffer.cid:"+getChannelId(),BuffersUtil.flipBuffers(tester.getBuffers()));
	}
	
	public void reciveBuffer(){
		asyncRead("reciveBuffer.cid:"+getChannelId());
	}
	
	/**
	 * コンストラクタから必ず呼び出す事
	 * @param coreTester
	 */
	protected void setCoreTester(CoreTester coreTester){
		this.coreTester=coreTester;
	}
	public void setBufferTester(BuffersTester tester){
		this.tester=tester;
	}
	
	public BuffersTester getBufferTester(){
		return tester;
	}

	@Override
	public void onAccepted(Object userContext) {
		this.name=this.getClass().getName()+ ":cid:"+getChannelId();
		coreTester.intoTest(this);
		logger.info("onAccepted:"+name);
	}

	@Override
	public void onConnected(Object userContext) {
		this.name=this.getClass().getName()+ ":cid:"+getChannelId();
		coreTester.intoTest(this);
		logger.info("onConnected:"+name);
	}

	@Override
	public void onRead(Object userContext, ByteBuffer[] buffers) {
		logger.info("onRead:"+name);
		readCount++;
		tester.putBuffer(buffers);
	}
	
	@Override
	public void onWritten(Object userContext) {
		logger.info("onWritten:"+name);
		writtenCount++;
	}
	
	@Override
	public void onClosed(Object userContext) {
		logger.info("onClosed:"+name+":userContext:"+userContext);
		closedCount++;
	}

	@Override
	public void onTimeout(Object userContext) {
		logger.info("onTimeout:"+name+":userContext:"+userContext);
		timeoutCount++;
		boolean ret=asyncClose("asyncClose from onTimeout:"+name);
		logger.info("asyncClose return:"+ret);
	}

	@Override
	public void onFailure(Object userContext,Throwable t) {
		logger.info("onFailure:"+name+":userContext:"+userContext,t);
		failureCount++;
		boolean ret=asyncClose("asyncClose from onFailure:"+name);
		logger.info("asyncClose return:"+ret);
	}
	
	/**
	 * onConnectFailureは、普通のonFailureとは違ってServerにリクエストが到着していない
	 */
	@Override
	public void onConnectFailure(Object userContext, Throwable t) {
		this.name=this.getClass().getName()+ ":cid:"+getChannelId();
		logger.info("onConnectFailure:"+name+":userContext:"+userContext,t);
		connectFailureCount++;
//		coreTester.outTest(this);
		boolean ret=asyncClose("asyncClose from onConnectFailure:"+name);
		logger.info("asyncClose return:"+ret);
	}

	@Override
	public void onFinished() {
		System.out.println("onFinished.cid:"+getChannelId()+":length:"+tester.getLength());
		if(tester!=null){
			logger.info("onFinished:"+name +":"+tester.getSeed());
		}else{
			logger.info("onFinished:"+name +":tester is null");
		}
		coreTester.outTest(this);
	}
	public int getReadCount() {
		return readCount;
	}
	public int getWrittenCount() {
		return writtenCount;
	}
	public int getClosedCount() {
		return closedCount;
	}
	public int getTimeoutCount() {
		return timeoutCount;
	}
	public int getFailureCount() {
		return failureCount;
	}
	public int getConnectFailureCount() {
		return connectFailureCount;
	}

}
