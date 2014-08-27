package naru.async.core;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreRW2Test extends TestBase{
	private static Logger logger=Logger.getLogger(CoreRW2Test.class);
	private static long DATA_LENGTH=10240;
	private static int  CHANNEL_COUNT=10;
	private static CoreTester coreTester;
	private static Object lock=new Object();
	private static List<TestServerHandler> servers=new ArrayList<TestServerHandler>();
	private static List<TestClientHandler> clients=new ArrayList<TestClientHandler>();
	private static int counter=0;
	
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
			setCoreTester(CoreRW2Test.coreTester);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			logger.info("onAccepted.cid:"+getChannelId());
//			if( !asyncRead("asyncRead:"+getChannelId()) ){//Šù‚ÉcloseÏ‚Ý
//			}
			super.onAccepted(userContext);
			synchronized(lock){
				servers.add(this);
				counter++;
				lock.notify();
			}
		}
		
		@Override
		public void onRead(ByteBuffer[] buffers, Object userContext) {
			super.onRead(buffers,userContext);
			logger.info("onRead.cid:"+getChannelId() + " length:"+tester.getLength());
			reciveBuffer();
		}
	}
	
	public static class TestClientHandler extends TestHandler{
		@Override
		public void onConnected(Object userContext) {
			setCoreTester(CoreRW2Test.coreTester);
			super.onConnected(userContext);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			System.out.println("onConnected.userContext:"+userContext);
			synchronized(lock){
				clients.add(this);
				counter++;
				lock.notify();
			}
//			asyncWrite("asyncWrite:"+getChannelId(),BuffersUtil.flipBuffers(tester.getBuffers()));
		}
		@Override
		public void onWritten(Object userContext) {
			super.onWritten(userContext);
			if(tester.getLength()>=DATA_LENGTH){
				asyncClose(userContext);
				return;
			}
			sendBuffer();
//			asyncWrite("asyncWrite:"+getChannelId(),BuffersUtil.flipBuffers(tester.getBuffers()));
		}
	}
	
	@Test
	public void test0() throws Throwable{
		callTest("qtest0");
	}
	public void qtest0() throws Throwable{
		DATA_LENGTH=Long.parseLong(System.getProperty("DATA_LENGTH"));
		CHANNEL_COUNT=Integer.parseInt(System.getProperty("CHANNEL_COUNT"));
		
		CoreRW2Test.coreTester=new CoreTester();
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept(TestServerHandler.class, address, 1024, "ChannelHandler.accept");
		for(int i=0;i<CHANNEL_COUNT;i++){
			if( ChannelHandler.connect(TestClientHandler.class, address, 1000, i)==null ){
				fail("ChannelHandler.connect fail");
			}
			synchronized(lock){
				while(counter<(i*2+2)){
					lock.wait();
				}
			}
		}
		
		int i=0;
		for(i=0;i<CHANNEL_COUNT;i++){
			TestServerHandler server=servers.get(i);
			server.reciveBuffer();
			TestClientHandler client=clients.get(i);
			client.sendBuffer();
		}
		for(;i<CHANNEL_COUNT;i++){
			TestServerHandler server=servers.get(i);
			server.asyncClose("server.asyncClose:cid:"+server.getChannelId());
		}
		
//		for(TestClientHandler client:clients){
//			client.asyncClose("close");
//		}
		coreTester.waitAndCheck(CHANNEL_COUNT*2);
		ah.asyncClose("test");
	}
}
