package naru.async.core;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreTimeoutTest extends TestBase{
	private static Logger logger=Logger.getLogger(CoreTimeoutTest.class);
	private static long DATA_LENGTH=10240;
	private static int  CHANNEL_COUNT=10;
	private static CoreTester coreTester;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestBase.setupContainer(
				"testEnv.properties",
				"CoreTest");
	}
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TestBase.stopContainer();
	}
	
	public static class TestReadClientHandler extends TestHandler{
		public TestReadClientHandler(){
			setCoreTester(CoreTimeoutTest.coreTester);
		}
		@Override
		public void onConnected(Object userContext) {
			super.onConnected(userContext);
			setReadTimeout(1234);
			System.out.println("onConnected.userContext:"+userContext);
			asyncRead("asyncRead:"+getChannelId());
		}
	}
	
	@Test
	public void testReadTimeout() throws Throwable{
		callTest("qtestReadTimeout");
	}
	public void qtestReadTimeout() throws Throwable{
		ServerSocket serverSocket=new ServerSocket(1234);
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		CoreTimeoutTest.coreTester=new CoreTester();
		TestReadClientHandler tch=(TestReadClientHandler)ChannelHandler.connect(TestReadClientHandler.class,"ChannelHandler.connect", address, 1000);
		if( tch==null ){
			fail("ChannelHandler.connect fail");
		}
		coreTester.waitAndCheck(1);
		assertEquals(1, tch.getTimeoutCount());
		serverSocket.close();
	}
	
	public static class TestWriteClientHandler extends TestHandler{
		public TestWriteClientHandler(){
			setCoreTester(CoreTimeoutTest.coreTester);
		}
		@Override
		public void onConnected(Object userContext) {
			super.onConnected(userContext);
			setWriteTimeout(1234);
			System.out.println("onConnected.userContext:"+userContext);
			asyncWrite("asyncWrite:"+getChannelId(),BuffersUtil.flipBuffers(tester.getBuffers()));
		}
		
		@Override
		public void onWritten(Object userContext) {
			super.onWritten(userContext);
			System.out.println("onWritten.userContext:"+userContext);
			asyncWrite("asyncWrite:"+getChannelId(),BuffersUtil.flipBuffers(tester.getBuffers()));
		}
	}
	
	
	@Test
	public void testWriteTimeout() throws Throwable{
		callTest("qtestWriteTimeout");
	}
	public void qtestWriteTimeout() throws Throwable{
		ServerSocket serverSocket=new ServerSocket(1234);
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		CoreTimeoutTest.coreTester=new CoreTester();
		TestWriteClientHandler tch=(TestWriteClientHandler)ChannelHandler.connect(TestWriteClientHandler.class,"ChannelHandler.connect", address, 1000);
		if( tch==null ){
			fail("ChannelHandler.connect fail");
		}
		coreTester.waitAndCheck(1);
		assertEquals(1, tch.getTimeoutCount());
		serverSocket.close();
	}
	
	public static class TestConnectClientHandler extends TestHandler{
		public TestConnectClientHandler(){
			setCoreTester(CoreTimeoutTest.coreTester);
		}
		//connect‚·‚é‚ª‰½‚à‚µ‚È‚¢
		@Override
		public void onConnected(Object userContext) {
			super.onConnected(userContext);
			setWriteTimeout(1234);
			System.out.println("onConnected.userContext:"+userContext);
		}
	}
	
	
	@Test
	public void testConnectTimeout() throws Throwable{
		callTest("qtestConnectTimeout");
	}
	public void qtestConnectTimeout() throws Throwable{
		ServerSocket serverSocket=new ServerSocket(1234);
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		CoreTimeoutTest.coreTester=new CoreTester();
		for(int i=0;i<1;i++){
			TestConnectClientHandler tch=(TestConnectClientHandler)ChannelHandler.connect(TestConnectClientHandler.class,"ChannelHandler.connect", address, 1000);
			if( tch==null ){
				fail("ChannelHandler.connect fail");
			}
		}
		serverSocket.close();
		coreTester.waitAndCheck(1);
	}

	/**
	 * open‚µ‚Ä‚¢‚È‚¢ƒ|[ƒg‚ÉÚ‘±
	 * @throws Throwable
	 */
	//@Test
	public void testConnectFail() throws Throwable{
		callTest("qtestConnectFail");
	}
	public void qtestConnectFail() throws Throwable{
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		CoreTimeoutTest.coreTester=new CoreTester();
		for(int i=0;i<1;i++){
			TestConnectClientHandler tch=(TestConnectClientHandler)ChannelHandler.connect(TestConnectClientHandler.class,"ChannelHandler.connect", address, 1000);
			if( tch==null ){
				fail("ChannelHandler.connect fail");
			}
		}
		coreTester.waitAndCheck(1);
	}
	
}
