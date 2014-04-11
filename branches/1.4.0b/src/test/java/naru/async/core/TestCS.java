package naru.async.core;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;

import naru.async.ChannelHandler;

public class TestCS {
	private InetSocketAddress selfServerAddress;
	private ChannelHandler acceptHandler = null;
	private ArrayList<TestClientHandler> clientHandles = new ArrayList<TestClientHandler>();
	private int maxConnectCount = 0;

	public TestCS(int port) throws UnknownHostException {
		InetAddress inetAdder = InetAddress.getLocalHost();
		selfServerAddress = new InetSocketAddress(inetAdder, port);
		acceptHandler = ChannelHandler.accept("test", selfServerAddress, 100,
				TestServerHandler.class);
	}

	public static class TestServerHandler extends ChannelHandler {
		@Override
		public void onAccepted(Object userContext) {
			System.out.println("TestServerHandler onAccepted.cid:"
					+ getChannelId());
		}

		@Override
		public void onTimeout(Object userContext) {
			System.out.println("TestServerHandler onTimeout.cid:"
					+ getChannelId());
			asyncClose(userContext);
		}

		@Override
		public void onFinished() {
			System.out.println("TestServerHandler onFinished.cid:"
					+ getChannelId());
		}

		@Override
		public void onFailure(Object userContext, Throwable t) {
			System.out.println("TestServerHandler onFailure.cid:"
					+ getChannelId());
			t.printStackTrace();
			asyncClose(userContext);
		}
	}

	public static class TestClientHandler extends ChannelHandler {
		boolean isFinished = false;
		boolean isConnected = false;

		@Override
		public void onConnected(Object userContext) {
			// cUserContext=userContext;
			System.out.println("TestClientHandler onConnected.cid:"
					+ getChannelId());
			synchronized (this) {
				isConnected = true;
				notify();
			}
		}

		@Override
		public void onFinished() {
			System.out.println("TestClientHandler onFinished.cid:"
					+ getChannelId());
			synchronized (this) {
				isFinished = true;
				notify();
			}
		}

		@Override
		public void onTimeout(Object userContext) {
			System.out.println("TestClientHandler onTimeout.cid:"
					+ getChannelId());
			asyncClose(userContext);
		}

		public boolean isFinished() {
			return isFinished;
		}

		public boolean isConnected() {
			return isConnected;
		}

		@Override
		public void onFailure(Object userContext, Throwable t) {
			System.out.println("TestClientHandler onFailure.cid:"
					+ getChannelId());
			t.printStackTrace();
			if (isConnected) {
				asyncClose(userContext);
			} else {
				onFinished();
			}
		}

		@Override
		public void recycle() {
			isFinished = false;
			isConnected = false;
			super.recycle();
		}
	}

	private boolean waitConnect(TestClientHandler handler) {
		synchronized (handler) {
			while (true) {
				if (handler.isConnected()) {
					return true;
				}
				if (handler.isFinished()) {
					return false;
				}
				try {
					handler.wait();
				} catch (InterruptedException e) {
				}
			}
		}
	}

	private boolean waitFinished(TestClientHandler handler) {
		synchronized (handler) {
			while (true) {
				if (handler.isFinished()) {
					return false;
				}
				try {
					handler.wait();
				} catch (InterruptedException e) {
				}
			}
		}
	}

	public boolean connects(String host, int port, int count)
			throws UnknownHostException {
		InetAddress inetAdder = InetAddress.getByName(host);
		InetSocketAddress address = new InetSocketAddress(inetAdder, port);
		return connects(address, count);
	}

	public boolean connects(int count) throws UnknownHostException {
		return connects(selfServerAddress, count);
	}

	private boolean connects(InetSocketAddress address, int count)
			throws UnknownHostException {
		long start = System.currentTimeMillis();
		int connectChannelCount = 0;
		for (int i = 0; i < count; i++) {
			TestClientHandler clientHandle = (TestClientHandler) ChannelHandler
					.connect(TestClientHandler.class, "test", address, 1000);
			if (clientHandle == null) {
				System.out.println("fail connect:" + i + ":time:"+ (System.currentTimeMillis() - start));
				disconnects();
				maxConnectCount = i - 1;
				return false;
			}
			System.out.println("connect cid:" + clientHandle.getChannelId());
			if (!waitConnect(clientHandle)) {
				System.out.println("fail connect:" + i + ":time:"+ (System.currentTimeMillis() - start));
				disconnects();
				maxConnectCount = i - 1;
				return false;
			}
			clientHandle.ref();
			clientHandles.add(clientHandle);
			connectChannelCount++;
		}
		System.out.println("success connect:" + count + ":time:"
				+ (System.currentTimeMillis() - start));
		maxConnectCount = count;
		return true;
	}

	public void disconnects() {
		long start = System.currentTimeMillis();
		if (clientHandles == null) {
			return;
		}
		Iterator<TestClientHandler> itr = clientHandles.iterator();
		while (itr.hasNext()) {
			TestClientHandler clientHandle = itr.next();
			System.out.println("asyncClose cid:" + clientHandle.getChannelId());
			clientHandle.asyncClose("test");
			waitFinished(clientHandle);
			itr.remove();
			clientHandle.unref();
		}
		acceptHandler.asyncClose("test");
		System.out.println("success disconnect:time:"
				+ (System.currentTimeMillis() - start));
	}

	public int getMaxConnectCount() {
		return maxConnectCount;
	}

	public ChannelHandler syncConnect() throws UnknownHostException {
		return syncConnect(selfServerAddress);
	}

	public ChannelHandler syncConnect(InetSocketAddress address)
			throws UnknownHostException {
		TestClientHandler clientHandler = (TestClientHandler) ChannelHandler
				.connect(TestClientHandler.class, "test", address, 1000);
		if (clientHandler == null) {
			System.out.println("fail connect1");
			return null;
		}
		System.out.println("connect cid:" + clientHandler.getChannelId());
		if (!waitConnect(clientHandler)) {
			System.out.println("fail connect2");
			disconnects();
			return null;
		}
		clientHandler.ref();
		return clientHandler;
	}

	public void waitFinish(ChannelHandler clientHandler) {
		waitFinished((TestClientHandler) clientHandler);
		clientHandler.unref();
	}

}