package naru.async.core;

import static org.junit.Assert.*;

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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreCloseTest extends TestBase{
	private static Logger logger=Logger.getLogger(CoreCloseTest.class);
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
	
	public static class TestServerHandler extends TestHandler{
		public TestServerHandler(){
			setCoreTester(CoreCloseTest.coreTester);
		}
		@Override
		public void onAccepted(Object userContext) {
			super.onAccepted(userContext);
			asyncClose("asyncClose:"+getChannelId());//いきなり切断
		}
	}
	
	public static class TestClientHandler extends TestHandler{
		public TestClientHandler(){
			setCoreTester(CoreCloseTest.coreTester);
		}
		@Override
		public void onConnected(Object userContext) {
			super.onConnected(userContext);
			setReadTimeout(1234);
			System.out.println("onConnected.userContext:"+userContext);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}//ちょっと待って
			asyncRead("asyncRead:"+getChannelId());
		}
	}
	
	/**
	 * リモートがcloseしたhandlerにasyncRead
	 * @throws Throwable
	 */
	@Test
	public void testCloseRead() throws Throwable{
		callTest("qtestCloseRead");
	}
	public void qtestCloseRead() throws Throwable{
		CoreCloseTest.coreTester=new CoreTester();
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept("ChannelHandler.accept", address, 1024, TestServerHandler.class);
		for(int i=0;i<1;i++){
			if( ChannelHandler.connect(TestClientHandler.class, i, address, 1000)==null ){
				fail("ChannelHandler.connect fail");
			}
		}
		coreTester.waitAndCheck(1*2);
		ah.asyncClose("test");
	}
	
	public static class TestWriteClientHandler extends TestHandler{
		public TestWriteClientHandler(){
			setCoreTester(CoreCloseTest.coreTester);
		}
		@Override
		public void onConnected(Object userContext) {
			super.onConnected(userContext);
			setReadTimeout(1234);
			System.out.println("onConnected.userContext:"+userContext);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}//ちょっと待って
			asyncWrite("asyncWrite:"+getChannelId(),tester.getBuffers());
		}
	}
	
	/**
	 * リモートがcloseしたhandlerにasyncWrite
	 * @throws Throwable
	 */
	@Test
	public void testCloseWrite() throws Throwable{
		callTest("qtestCloseWrite");
	}
	public void qtestCloseWrite() throws Throwable{
		CoreCloseTest.coreTester=new CoreTester();
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept("ChannelHandler.accept", address, 1024, TestServerHandler.class);
		for(int i=0;i<1;i++){
			if( ChannelHandler.connect(TestWriteClientHandler.class, i, address, 1000)==null ){
				fail("ChannelHandler.connect fail");
			}
		}
		coreTester.waitAndCheck(1*2);
		ah.asyncClose("test");
	}
	
	public static class TestNotClientHandler extends TestHandler{
		public TestNotClientHandler(){
			setCoreTester(CoreCloseTest.coreTester);
		}
		@Override
		public void onConnected(Object userContext) {
			super.onConnected(userContext);
			setReadTimeout(1234);
			System.out.println("onConnected.userContext:"+userContext);
			//何もしない
		}
	}
	
	/**
	 * リモートがcloseしたhandlerで後放置すると
	 * @throws Throwable
	 */
	@Test
	public void testCloseNot() throws Throwable{
		callTest("qtestCloseNot");
	}
	public void qtestCloseNot() throws Throwable{
		CoreCloseTest.coreTester=new CoreTester();
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept("ChannelHandler.accept", address, 1024, TestServerHandler.class);
		for(int i=0;i<1;i++){
			if( ChannelHandler.connect(TestNotClientHandler.class, i, address, 1000)==null ){
				fail("ChannelHandler.connect fail");
			}
		}
		coreTester.waitAndCheck(1*2);
		ah.asyncClose("test");
	}
}
