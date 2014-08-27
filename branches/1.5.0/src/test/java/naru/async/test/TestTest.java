package naru.async.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import naru.async.BufferGenerator;
import naru.async.ChannelHandler;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTest extends TestBase{
	private static Logger logger=Logger.getLogger(TestTest.class);
	
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
	
	@Test
	public void testListenClose() throws Throwable{
		callTest("qtestListenClose",Long.MAX_VALUE);
	}
	public void qtestListenClose() throws Throwable{
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 9876);
		ChannelHandler selectHandler=ChannelHandler.allocHandler(TestListenHandler.class);
		selectHandler.asyncAccept(address, 1024, TestAcceptHandler.class, "TestListenHandler asyncAccept test");
		selectHandler.asyncClose("TestListenHandler asyncClose test");
	}
	
	@Test
	public void testConnectClose() throws Throwable{
		callTest("qtestConnectClose",Long.MAX_VALUE);
	}
	public void qtestConnectClose() throws Throwable{
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 9876);
		ChannelHandler selectHandler=ChannelHandler.allocHandler(TestListenHandler.class);
		ChannelHandler connectHandler=ChannelHandler.allocHandler(TestConnectHandler.class);
		selectHandler.asyncAccept(address, 1024, TestAcceptHandler.class, "TestAcceptHandler asyncAccept test");
		connectHandler.asyncConnect(address, 1000, "TestConnect asyncConnect test");
		selectHandler.asyncClose("TestListenHandler asyncClose test");
		connectHandler.asyncClose("TestConnectHandler asyncClose test");
		Thread.sleep(10000);
	}
	
	@Test
	public void testWriteClose() throws Throwable{
		callTest("qtestWriteClose",Long.MAX_VALUE);
	}
	public void qtestWriteClose() throws Throwable{
		BufferGenerator bg=new BufferGenerator(1024,1);
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 9876);
		ChannelHandler selectHandler=ChannelHandler.allocHandler(TestListenHandler.class);
		ChannelHandler connectHandler=ChannelHandler.allocHandler(TestConnectHandler.class);
		selectHandler.asyncAccept(address, 1024, TestAcceptHandler.class, "TestAcceptHandler asyncAccept test");
		connectHandler.asyncConnect(address, 1000, "TestConnect asyncConnect test");
		selectHandler.asyncClose("TestListenHandler asyncClose test");
		
		connectHandler.asyncWrite(bg.getBuffer(), "TestConnectHandler asyncWrite test");
		bg.term();
		//Thread.sleep(10000);
		connectHandler.asyncClose("TestConnectHandler asyncClose test");
		Thread.sleep(10000);
	}
	
	
}
