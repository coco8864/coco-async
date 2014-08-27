package naru.async.ssl;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import naru.async.BuffersTester;
import naru.async.ChannelHandler;
import naru.async.core.CoreTester;

public abstract class TestSslHandler extends SslHandler {
	private static Logger logger=Logger.getLogger(TestSslHandler.class);
	
	protected BuffersTester tester;
	private SslTester sslTester;
	private String name;
	private int readPlainCount;
	private int writtenPlainCount;
	private int closedCount;
	private int timeoutCount;
	private int failureCount;
	private int connectFailureCount;
	
	/**
	 * コンストラクタから必ず呼び出す事
	 * @param coreTester
	 */
	protected void setCoreTester(SslTester sslTester){
		this.sslTester=sslTester;
	}
	public void setBufferTester(BuffersTester tester){
		this.tester=tester;
		this.name=this.getClass().getName()+ ":cid:"+getChannelId();
	}
	
	public BuffersTester getBufferTester(){
		return tester;
	}

	@Override
	public void onAccepted(Object userContext) {
		sslTester.intoTest(this);
		logger.info("onAccepted:"+name);
	}

	@Override
	public void onConnected(Object userContext) {
		sslTester.intoTest(this);
		logger.info("onConnected:"+name);
	}

	@Override
	public void onReadPlain(ByteBuffer[] buffers, Object userContext) {
		logger.info("onRead:"+name);
		readPlainCount++;
		tester.putBuffer(buffers);
	}
	
	@Override
	public void onWrittenPlain(Object userContext) {
		logger.info("onWrittenPlain:"+name);
		writtenPlainCount++;
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
		asyncClose(userContext);
	}

	@Override
	public void onFailure(Throwable t,Object userContext) {
		logger.info("onFailure:"+name+":userContext:"+userContext,t);
		failureCount++;
		asyncClose(userContext);
	}
	
	/**
	 * onConnectFailureは、普通のonFailureとは違ってServerにリクエストが到着していない
	 */
	@Override
	public void onConnectFailure(Throwable t, Object userContext) {
		logger.info("onConnectFailure:"+name+":userContext:"+userContext,t);
		connectFailureCount++;
		asyncClose(userContext);
	}

	@Override
	public void onFinished() {
		if(tester!=null){
			logger.info("onFinished:"+name +":"+tester.getSeed());
		}else{
			logger.info("onFinished:"+name +":tester is null");
		}
		sslTester.outTest(this);
	}
	public int getReadPlainCount() {
		return readPlainCount;
	}
	public int getWrittenPlainCount() {
		return writtenPlainCount;
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
