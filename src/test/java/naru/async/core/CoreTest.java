package naru.async.core;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import naru.async.ChannelHandler;
import naru.async.core.CoreRWTest.TestClientHandler;
import naru.async.core.CoreRWTest.TestServerHandler;
import naru.async.pool.BuffersUtil;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreTest extends TestBase{
	private static Logger logger=Logger.getLogger(CoreTest.class);
	private static CoreTester coreTester;
	
	@BeforeClass
	public static void beforClass() throws IOException {
		setupContainer();
	}
	@Before
	public void beforeTest() {
		//storeÇèâä˙âªÇ∑ÇÈÉRÅ[Éh
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
		public TestServerHandler(){
			setCoreTester(CoreTest.coreTester);
		}
		
		@Override
		public void onAccepted(Object userContext) {
//			setReadTimeout(1000);
			setWriteTimeout(1000);
			logger.info("onAccepted.cid:"+getChannelId());
			if( !asyncRead("asyncRead:"+getChannelId()) ){//ä˘Ç…closeçœÇ›
			}
			super.onAccepted(userContext);
		}
	}
	
	public static class TestClientHandler extends TestHandler{
		public TestClientHandler(){
			setCoreTester(CoreTest.coreTester);
		}
		@Override
		public void onConnected(Object userContext) {
			super.onConnected(userContext);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			asyncRead(null);
			System.out.println("onConnected.userContext:"+userContext);
		}
	}
	
	@Test
	public void testReadTimeout() throws Throwable{
		callTest("qtestReadTimeout",Long.MAX_VALUE);
	}
	public void qtestReadTimeout() throws Throwable{
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept(TestServerHandler.class, address, 1024, "ChannelHandler.accept");
		CoreTest.coreTester=new CoreTester();
		int i=0;
		for(i=0;i<10;i++){
			Thread.sleep(50);
			if( ChannelHandler.connect(TestClientHandler.class, address, 1000, i)==null ){
				fail("ChannelHandler.connect fail");
			}
		}
		coreTester.waitAndCheck(i*2);
		ah.asyncClose("test");
		Thread.sleep(2000);
	}
}
