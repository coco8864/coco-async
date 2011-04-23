package naru.async.core;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreRWTest extends TestBase{
	private static Logger logger=Logger.getLogger(CoreRWTest.class);
	private static long DATA_LENGTH=10240;
	private static int  CHANNEL_COUNT=10;
	private static CoreTester coreTester;
	
	@BeforeClass
	public static void beforClass() throws IOException {
		setupContainer();
	}
	@Before
	public void beforeTest() {
		//store‚ð‰Šú‰»‚·‚éƒR[ƒh
		String storeDir=getProperty("storeDir");
		if(storeDir==null){
			throw new IllegalStateException("fail to get storeDir");
		}
		File dir=new File(storeDir);
		if(dir.exists()){
			File persistenceStore=new File(dir,"persistenceStore.sar");
			if(persistenceStore.exists()){
				persistenceStore.delete();
			}
		}else{
			dir.mkdir();
		}
		System.out.println("storeDir:"+storeDir);
		System.out.println("Queuelet container start");
		startContainer("CoreTest");
	}
	
	@After
	public void afterTest() {
		stopContainer();
		System.out.println("Queuelet container stop");
	}
	
	public static class TestServerHandler extends TestHandler{
		@Override
		public void onAccepted(Object userContext) {
			System.out.println("onAccepted.userContext:"+userContext);
			setCoreTester(CoreRWTest.coreTester);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			logger.info("onAccepted.cid:"+getChannelId());
			if( !asyncRead("asyncRead:"+getChannelId()) ){//Šù‚ÉcloseÏ‚Ý
			}
			super.onAccepted(userContext);
		}
		
		@Override
		public void onRead(Object userContext, ByteBuffer[] buffers) {
			super.onRead(userContext,buffers);
			logger.info("onRead.cid:"+getChannelId() + " length:"+tester.getLength());
			if( !asyncRead("asyncRead:"+getChannelId()) ){//Šù‚ÉcloseÏ‚Ý
			}
		}
	}
	
	public static class TestClientHandler extends TestHandler{
		@Override
		public void onConnected(Object userContext) {
			setCoreTester(CoreRWTest.coreTester);
			super.onConnected(userContext);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			System.out.println("onConnected.userContext:"+userContext);
			asyncWrite("asyncWrite:"+getChannelId(),BuffersUtil.flipBuffers(tester.getBuffers()));
		}
		@Override
		public void onWritten(Object userContext) {
			super.onWritten(userContext);
			if(tester.getLength()>=DATA_LENGTH){
				asyncClose(userContext);
				return;
			}
			asyncWrite("asyncWrite:"+getChannelId(),BuffersUtil.flipBuffers(tester.getBuffers()));
		}
	}
	
	@Test
	public void test0() throws Throwable{
		callTest("qtest0");
	}
	public void qtest0() throws Throwable{
		DATA_LENGTH=Long.parseLong(getProperty("DATA_LENGTH"));
		CHANNEL_COUNT=Integer.parseInt(getProperty("CHANNEL_COUNT"));
		
		CoreRWTest.coreTester=new CoreTester();
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept("ChannelHandler.accept", address, 1024, TestServerHandler.class);
		for(int i=0;i<CHANNEL_COUNT;i++){
			if( ChannelHandler.connect(TestClientHandler.class, i, address, 1000)==null ){
				fail("ChannelHandler.connect fail");
			}
			Thread.sleep(10);
		}
		coreTester.waitAndCheck(CHANNEL_COUNT*2);
		ah.asyncClose("test");
	}
}
