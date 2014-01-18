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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;
import naru.queuelet.test.TestBase;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SslRWTest extends TestBase{
	private static Logger logger=Logger.getLogger(SslRWTest.class);
	private static SSLContext sslContext;
	private static long DATA_LENGTH;
	private static int  CHANNEL_COUNT;
	private static SslTester sslTester;
	
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
	
	public static class TestServerHandler extends TestSslHandler{
		
		@Override
		public void onAccepted(Object userContext) {
			setCoreTester(SslRWTest.sslTester);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			sslOpen(false);
			super.onAccepted(userContext);
		}
		
		@Override
		public boolean onHandshaked() {
			return true;
		}
		
		@Override
		public void onReadPlain(Object userContext, ByteBuffer[] buffers) {
			super.onReadPlain(userContext,buffers);
			logger.info("onRead.cid:"+getChannelId());
			if( !asyncRead(userContext) ){//既にclose済み
				//asyncClose(userContext);
			}
		}

		@Override
		public SSLEngine getSSLEngine() {
			return sslContext.createSSLEngine(null, 1234);
		}
	}
	
	public static class TestClientHandler extends TestSslHandler{
		@Override
		public void onConnected(Object userContext) {
			setCoreTester(SslRWTest.sslTester);
			setReadTimeout(1000);
			setWriteTimeout(1000);
			System.out.println("onConnected.userContext:"+userContext);
			sslOpen(true);
			super.onConnected(userContext);
		}
		
		
		@Override
		public boolean onHandshaked() {
			asyncWrite("onHandshaked",BuffersUtil.flipBuffers(tester.getBuffers()));
			return false;
		}

		@Override
		public void onWrittenPlain(Object userContext) {
			super.onWrittenPlain(userContext);
			if(tester.getLength()>=DATA_LENGTH){
				asyncClose(userContext);
				return;
			}
			asyncWrite(userContext,BuffersUtil.flipBuffers(tester.getBuffers()));
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
		DATA_LENGTH=Long.parseLong(System.getProperty("DATA_LENGTH"));
		CHANNEL_COUNT=Integer.parseInt(System.getProperty("CHANNEL_COUNT"));
		
		InetAddress inetAdder=InetAddress.getLocalHost();
		InetSocketAddress address=new InetSocketAddress(inetAdder, 1234);
		ChannelHandler ah=ChannelHandler.accept("test", address, 1024, TestServerHandler.class);
		SslRWTest.sslTester=new SslTester();
		for(int i=0;i<CHANNEL_COUNT;i++){
			if( ChannelHandler.connect(TestClientHandler.class, i, address, 1000)==null ){
				fail("ChannelHandler.connect fail");
			}
		}
		sslTester.waitAndCheck(CHANNEL_COUNT);
		ah.asyncClose("test");
	}
}
