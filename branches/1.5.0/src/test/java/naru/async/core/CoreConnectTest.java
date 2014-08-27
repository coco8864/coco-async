package naru.async.core;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import naru.async.ChannelHandler;
import naru.queuelet.test.TestBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreConnectTest extends TestBase{
	@BeforeClass
	public static void beforClass() throws IOException {
		setupContainer();
	}
	@Before
	public void beforeTest() {
		//storeを初期化するコード
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
	public void afterClass() {
		stopContainer();
		System.out.println("Queuelet container stop");
	}
	
	private static Object lock=new Object();
	private static int calledCount=0;
	private static void called(){
		synchronized(lock){
			calledCount++;
			lock.notify();
		}
	}
	
	public static class TestServerHandler extends ChannelHandler{
		@Override
		public void onAccepted(Object userContext) {
			System.out.println("TestServerHandler onAccepted");
			called();
		}
		@Override
		public void onTimeout(Object userContext) {
			System.out.println("TestServerHandler onTimeout");
			super.onTimeout(userContext);
		}
		@Override
		public void onFinished() {
			System.out.println("TestServerHandler onFinished");
			called();
		}
	}
	
	public static class TestClientHandler extends ChannelHandler{
		boolean isTimeout=false;
		boolean isFinished=false;
		boolean isConnect=false;
		private Object cUserContext;
		private long connectTime;
		@Override
		public void onConnected(Object userContext) {
			cUserContext=userContext;
			connectTime=System.currentTimeMillis();
//			System.out.println("TestClientHandler onConnected.userContext:"+userContext);
			asyncWrite(new ByteBuffer[]{ByteBuffer.wrap("GET".getBytes())},userContext);
//			asyncClose(userContext);//いきなりクローズ
			synchronized(userContext){
				userContext.notify();
				isConnect=true;
			}
		}
		@Override
		public void onFinished() {
			System.out.println("TestClientHandler onFinished.cUserContext:"+cUserContext +":" +(System.currentTimeMillis()-connectTime));
			synchronized(this){
				isFinished=true;
				isConnect=false;
				notify();
			}
		}
		@Override
		public void onTimeout(Object userContext) {
			System.out.println("TestClientHandler onTimeout.cUserContext:"+cUserContext);
			synchronized(userContext){
				isTimeout=true;
				userContext.notify();
			}
		}
		public boolean isTimeout(){
			return isTimeout;
		}
		public boolean isFinished(){
			return isFinished;
		}
	}
	
	@Test
	public void test0() throws Throwable{
		callTest("qtest0",Long.MAX_VALUE);
	}
	public void qtest0() throws Throwable{
		TestCS handlers=new TestCS(1234);
		int callCount=Integer.MAX_VALUE;
		long start=System.currentTimeMillis();
		if( handlers.connects(callCount) ){
			handlers.disconnects();
		}
		System.out.println("maxConnectCount:"+handlers.getMaxConnectCount()+":time:"+(System.currentTimeMillis()-start));
	}
	
	@Test
	public void test1() throws Throwable{
		callTest("qtest1",Long.MAX_VALUE);
	}
	public void qtest1() throws Throwable{
		TestCS handlers=new TestCS(1235);
		int callCount=100;
		if( !handlers.connects(callCount) ){
			fail("fail to connect callCount:"+callCount);
		}
		long start=System.currentTimeMillis();
		for(int i=0;i<100;i++){
			System.out.println("i:"+i);
			ChannelHandler handler=handlers.syncConnect();
			if(handler==null){
				handlers.disconnects();
				fail("fail to connect callCount:"+callCount);
			}
			handler.asyncClose("test");
			handlers.waitFinish(handler);
		}
		System.out.println("===time:"+(System.currentTimeMillis()-start));
		handlers.disconnects();
	}
	
	@Test
	public void test3() throws Throwable{
		callTest("qtest3",Long.MAX_VALUE);
	}
	public void qtest3() throws Throwable{
		System.out.println("i am qtest3");
		TestClientHandler[] tchs=new TestClientHandler[10240];
		for(int i=0;i<tchs.length;i++){
			tchs[i]=new TestClientHandler();
		}
		for(int i=0;i<tchs.length;i++){
//			tchs[i].asyncConnect("test", "judus.soft.fujitsu.com", 1280, 100000);
			tchs[i].asyncConnect("localhost", 1280, 100000, "test");
//			Thread.sleep(1);
		}
		int count=0;
		for(int i=0;i<tchs.length;i++){
			if( tchs[i].isConnect ){
				count++;
			}
			tchs[i].asyncClose("test");
		}
		System.out.println("count:" +count + "/" +tchs.length);
		
	}
	
}
