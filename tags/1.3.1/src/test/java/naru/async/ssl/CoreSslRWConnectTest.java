package naru.async.ssl;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import naru.async.BuffersTester;
import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreSslRWConnectTest extends TestBase{
	private static Logger logger=Logger.getLogger(CoreSslRWConnectTest.class);
	private static SSLContext sslContext;
	
	private static class MyTrustManager implements X509TrustManager {
		private X509TrustManager sunJSSEX509TrustManager;

		public MyTrustManager(KeyStore ks) throws NoSuchAlgorithmException, NoSuchProviderException, KeyStoreException {
			// create a "default" JSSE X509TrustManager.
//			KeyStore ks = KeyStore.getInstance("JKS");
//			ks.load(new FileInputStream("trustedCerts"), "passphrase".toCharArray());
			TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509","SunJSSE");
			tmf.init(ks);
			TrustManager tms[] = tmf.getTrustManagers();
			/*
			 * Iterate over the returned trustmanagers, look for an instance of
			 * X509TrustManager. If found, use that as our "default" trust manager.
			 */
			for (int i = 0; i < tms.length; i++) {
				if (tms[i] instanceof X509TrustManager) {
					sunJSSEX509TrustManager = (X509TrustManager) tms[i];
					return;
				}
			}
			/*
			 * Find some other way to initialize, or else we have to fail the
			 * constructor.
			 */
			throw new IllegalStateException("Couldn't initialize");
		}

		public void checkClientTrusted(X509Certificate[] certs, String authType)
				throws CertificateException {
			try {
				sunJSSEX509TrustManager.checkClientTrusted(certs, authType);
			} catch (CertificateException e) {
				logger.warn("checkClientTrusted fail",e);
				//TODO 本当は、ユーザ毎に証明書の管理をすべき
				//Userテーブルには、証明書の格納場所は作ってある
//				throw e;
			}
		}

		public void checkServerTrusted(X509Certificate[] certs, String authType)
				throws CertificateException {
			try {
				sunJSSEX509TrustManager.checkServerTrusted(certs, authType);
			} catch (CertificateException e) {
				logger.warn("checkServerTrusted fail",e);
//				throw e;
			}
		}
		public X509Certificate[] getAcceptedIssuers() {
			return sunJSSEX509TrustManager.getAcceptedIssuers();
		}
	}
	
	static{
		String trustStorePassword =System.getProperty("trustStorePassword");;
		String trustStore=System.getProperty("trustStore");
		InputStream is=null;
		try {
			if(trustStorePassword!=null && trustStore!=null){
			KeyStore ks = KeyStore.getInstance("JKS");
			char[] keystorePass = trustStorePassword.toCharArray();
			is=new FileInputStream(trustStore);
			ks.load(is, keystorePass);
			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
			kmf.init(ks, keystorePass);
			TrustManager[] tms=new TrustManager[]{new MyTrustManager(ks)};
			sslContext = SSLContext.getInstance("TLS");
			sslContext.init(kmf.getKeyManagers(), tms, null);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyStoreException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CertificateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnrecoverableKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchProviderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(is!=null){
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
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
	
	private static List<BuffersTester> testers=new ArrayList<BuffersTester>();
	private static Set<Long>cids=Collections.synchronizedSet(new HashSet<Long>());
	private static Object lock=new Object();
	private static int calledCount=0;
	private static void called(){
		synchronized(lock){
			calledCount++;
			lock.notify();
		}
	}
	
	public static class TestServerHandler extends SslHandler{
		private BuffersTester tester;//=new BuffersTester();
		
		@Override
		public void recycle() {
			tester=new BuffersTester();
			super.recycle();
		}

		@Override
		public void onAccepted(Object userContext) {
			cids.add(getChannelId());
//			sslOpen(false);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			logger.info("onAccepted.cid:"+getChannelId() + " length:"+tester.getLength());
			System.out.println("onAccepted"+ " length:"+tester.getLength());
			sslOpen(false);
//			if( !asyncRead(userContext) ){//既にclose済み
//			}
		}
		
		@Override
		public boolean onHandshaked() {
			return true;
		}

		/*
		private boolean isFirstRead=true;
		@Override
		public void onRead(Object userContext, ByteBuffer[] buffers) {
			if(isFirstRead){
				if( !sslOpenWithBuffer(false, buffers) ){
					System.out.println("sslOpenWithBuffer error");
					asyncClose(null);//handshake失敗
				}
				isFirstRead=false;
			}else{
				super.onRead(userContext, buffers);
			}
			return;
		}
		*/
		
		
		@Override
		public void onReadPlain(Object userContext, ByteBuffer[] buffers) {
			tester.putBuffer(buffers);
			logger.info("onRead.cid:"+getChannelId() + " length:"+tester.getLength());
			if( !asyncRead(userContext) ){//既にclose済み
				//asyncClose(userContext);
			}
		}
		@Override
		public void onFailure(Object userContext, Throwable t) {
			System.out.println("TestServerHandler onFailure.cid:"+getChannelId());
			t.printStackTrace();
			logger.info("TestServerHandler onFailure.cid:"+getChannelId(),t);
		}

		@Override
		public void onFinished() {
			logger.info("TestServerHandler onFinished.cid:"+getChannelId() + " length:"+tester.getLength());
			System.out.println("TestServerHandler onFinished");
			testers.add(tester);
			cids.remove(getChannelId());
			called();
		}

		@Override
		public SSLEngine getSSLEngine() {
			return sslContext.createSSLEngine(null, 1234);
		}
	}
	
	public static class TestClientHandler extends SslHandler{
//		private BuffersTester tester=new BuffersTester();
		private BuffersTester tester;//=new BuffersTester();
		@Override
		public void recycle() {
			tester=new BuffersTester();
			super.recycle();
		}
		
		@Override
		public void onConnected(Object userContext) {
			setReadTimeout(1000);
			setWriteTimeout(1000);
			cids.add(getChannelId());
			System.out.println("onConnected.userContext:"+userContext);
			sslOpen(true);
		}
		
		
		@Override
		public boolean onHandshaked() {
			asyncWrite("onHandshaked",BuffersUtil.flipBuffers(tester.getBuffers()));
			return false;
		}

		@Override
		public void onWrittenPlain(Object userContext) {
			if(tester.getLength()>=2048){
				asyncClose(userContext);
				return;
			}
			asyncWrite(userContext,BuffersUtil.flipBuffers(tester.getBuffers()));
		}
		@Override
		public void onFinished() {
			logger.info("TestClientHandler onFinished.cid:"+getChannelId() + " length:"+tester.getLength());
			System.out.println("TestClientHandler onFinished");
			testers.add(tester);
			cids.remove(getChannelId());
			called();
		}

		@Override
		public void onFailure(Object userContext, Throwable t) {
			System.out.println("TestClientHandler onFailure.cid:"+getChannelId());
			t.printStackTrace();
			logger.info("TestClientHandler onFailure.cid:"+getChannelId(),t);
		}

		@Override
		public SSLEngine getSSLEngine() {
			return sslContext.createSSLEngine(null, 1234);
		}
	}
	
	@Test
	public void test0() throws Throwable{
		callTest("qtest0");
	}
	public void qtest0() throws Throwable{
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept("test", address, 1024, TestServerHandler.class);
		long start=System.currentTimeMillis();
//		int callCount=512; //NG,accept以降待てど暮らせどread可能にならないsocketがある。
//		int callCount=256;
		int callCount=32; //OK
//		int callCount=1; //OK
		for(int i=0;i<callCount;i++){
			ChannelHandler.connect(TestClientHandler.class, i, address, 1000);
//			Thread.sleep(1000);
		}
		synchronized(lock){
			while(true){
				if(callCount*2<=calledCount){
					break;
				}
				lock.wait(1000);
				System.out.println("cids:"+cids);
			}
		}
		System.out.println("X calledCount:"+calledCount);
		System.out.println("time:" +(System.currentTimeMillis()-start));
		assertEquals(callCount*2, calledCount);
		for(BuffersTester tester:testers){
			System.out.println(tester.getLength());
			try{
				tester.check();
			}catch(Throwable e){
				e.printStackTrace();
			}
		}
		ah.asyncClose("test");
	}
}
