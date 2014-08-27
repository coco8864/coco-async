package naru.async.test;

import java.nio.ByteBuffer;
import org.apache.log4j.Logger;
import naru.async.ChannelHandler;

public class TestConnectHandler extends ChannelHandler {
	private static Logger logger=Logger.getLogger(TestConnectHandler.class);

	@Override
	public void onAcceptable(Object userContext) {
		logger.info("onAcceptable");
	}

	@Override
	public void onAccepted(Object userContext) {
		logger.info("onAccepted");
	}

	@Override
	public void onConnected(Object userContext) {
		logger.info("onConnected");
	}

	@Override
	public void onRead(ByteBuffer[] buffers, Object userContext) {
		logger.info("onRead");
	}

	@Override
	public void onWritten(Object userContext) {
		logger.info("onWritten");
	}

	@Override
	public void onClosed() {
		logger.info("onClosed");
	}

	@Override
	public void onClosed(Object userContext) {
		logger.info("onClosed");
	}

	@Override
	public void onWriteClosed(Object[] userContexts) {
		logger.info("onWriteClosed");
	}

	@Override
	public void onReadClosed(Object userContext) {
		logger.info("onReadClosed");
	}

	@Override
	public void onAcceptClosed(Object userContext) {
		logger.info("onAcceptClosed");
	}

	@Override
	public void onConnectClosed(Object userContext) {
		logger.info("onConnectClosed");
	}

	@Override
	public void onCloseClosed(Object userContext) {
		logger.info("onCloseClosed");
	}

	@Override
	public void onFailure(Throwable t) {
		logger.info("onFailure");
	}

	@Override
	public void onFailure(Throwable t, Object userContext) {
		logger.info("onFailure");
	}

	@Override
	public void onWriteFailure(Throwable t, Object[] userContexts) {
		logger.info("onWriteFailure");
	}

	@Override
	public void onReadFailure(Throwable t, Object userContext) {
		logger.info("onReadFailure");
	}

	@Override
	public void onAcceptFailure(Throwable t, Object userContext) {
		logger.info("onAcceptFailure");
	}

	@Override
	public void onConnectFailure(Throwable t, Object userContext) {
		logger.info("onConnectFailure");
	}

	@Override
	public void onCloseFailure(Throwable t, Object userContext) {
		logger.info("onCloseFailure");
	}

	@Override
	public void onTimeout() {
		logger.info("onTimeout");
	}

	@Override
	public void onTimeout(Object userContext) {
		logger.info("onTimeout");
	}

	@Override
	public void onReadTimeout(Object userContext) {
		logger.info("onReadTimeout");
	}

	@Override
	public void onWriteTimeout(Object[] userContexts) {
		logger.info("onWriteTimeout");
	}

	@Override
	public void onConnectTimeout(Object userContext) {
		logger.info("onConnectTimeout");
	}

	@Override
	public void onFinished() {
		logger.info("onFinished");
	}

}
