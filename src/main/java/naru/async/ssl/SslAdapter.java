package naru.async.ssl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

import org.apache.log4j.Logger;

public class SslAdapter extends PoolBase {
	public static final String SSLCTX_READ_NETWORK = "readNetwork";
	public static final String SSLCTX_WRITE_NETWORK = "writeNetwork";
	public static final String SSLCTX_CLOSE_NETWORK = "CloseNetwork";
	public static final String SSLCTX_PLAIN_DATA = "plainData";
	public static final String SSLCTX_DUMMY_USER_CONTEXT = "dummyUserContext";

	private static Logger logger = Logger.getLogger(SslAdapter.class);
	private static ByteBuffer dmmyBuffer = ByteBuffer.allocate(0);
//	private static ByteBuffer[] dmmyBuffers = new ByteBuffer[] { dmmyBuffer };
	private static ByteBuffer[] dmmyBuffers = new ByteBuffer[0];// { dmmyBuffer };

	private SSLEngine sslEngine;
	private SSLEngineResult sslResult;
	private int packetSize;
	private LinkedList<ByteBuffer> networkBuffers = new LinkedList<ByteBuffer>();
	private boolean isHandshake = false;
	private SslHandler handler;

	public void recycle() {
		sslEngine = null;
		sslResult = null;
		handler = null;
		PoolManager.poolBufferInstance(networkBuffers);
//		networkBuffers.clear();
		isHandshake = false;
		onWrittenCounter = networkWriteCounter = 0;
		onWrittenMap.clear();
	}

	/* �擪�S�o�C�g�����āA�R���g���[���������܂܂�Ă����SSL�ʐM�Ɣ��f */
	public static boolean isSsl(ByteBuffer byteBuffer) {
		int n = 4;
		if (n > byteBuffer.limit()) {
			n = byteBuffer.limit();
		}
		for (int i = 0; i < n; i++) {
			byte c = byteBuffer.get(i);
			if (Character.isISOControl(c)) {
				return true;
			}
		}
		return false;
	}

	public boolean openWithBuffer(boolean isClientMode, SslHandler handler,
			ByteBuffer[] buffers) {
		for (int i = 0; i < buffers.length; i++) {
			networkBuffers.add(buffers[i]);
		}
		//�z���ԋp
		PoolManager.poolArrayInstance(buffers);
		return open(isClientMode, handler);
	}

	public boolean open(boolean isClientMode, SslHandler handler) {
		this.handler = handler;

		sslEngine = handler.getSSLEngine();// Setting.getSslEngine(null, 443);
		sslEngine.setUseClientMode(isClientMode);
		SSLSession session = sslEngine.getSession();
		packetSize = session.getPacketBufferSize();
		if (isClientMode) {
			try {
				asyncWrite(SSLCTX_PLAIN_DATA, dmmyBuffers);
				if (handshake()) {
					// �����Ȃ�handshake����������̂͑z��O
					logger.error("illigal handshake.");
					return false;
				}
			} catch (SSLException e) {
				logger.error("SslContext client open error.", e);
				return false;
			}
		} else if (networkBuffers.size() > 0) {
			try {
				ByteBuffer src = nextUnwrapBuffer();
				ByteBuffer dst = unwrap(src);
				// handshake����unwrap���ʂ̕K�v�Ȃ�
				if (dst != null) {
					PoolManager.poolBufferInstance(dst);
				}
				if (handshake()) {
					// �����Ȃ�handshake����������̂͑z��O
					logger.error("illigal handshake.");
					return false;
				}
			} catch (SSLException e) {//�ؖ����m�F��ʂ��o��ꍇ�Ȃǐ���n�ł���
				logger.debug("SslContext server open error.", e);
				return false;
			}
		} else {
			handler.asyncRead(SSLCTX_READ_NETWORK);
		}
		return true;
	}

	private Object lock = new Object();

	public void closeOutbound() {
		synchronized (lock) {
			if (sslEngine == null) {
				return;
			}
			try {
				sslEngine.closeOutbound();
				asyncWrite(SSLCTX_PLAIN_DATA, dmmyBuffers);
			} catch (SSLException ignore) {
				logger.warn("SslContext closeOutbound error.", ignore);
			}
			// handler.asyncClose(SSLCTX_NETWORK_DATA);
			sslEngine = null;
		}
	}

	public void closeInbound() {
		synchronized (lock) {
			if (sslEngine == null) {
				return;
			}
			try {
				if (sslEngine.isInboundDone()) {
					return;// ����ȏ�A�v���P�[�V�����f�[�^�𐶐����Ȃ��ꍇ
				}
				sslEngine.closeInbound();
				onRead(SSLCTX_PLAIN_DATA, dmmyBuffers);
			} catch (SSLException ignore) {
				logger.debug("SslContext closeInbound error.", ignore);
			}
		}
	}

	public void setHandler(SslHandler handler) {
		this.handler = handler;
	}

	private ByteBuffer nextUnwrapBuffer() {
		int size = networkBuffers.size();
		if (size == 0) {
			return dmmyBuffer;
		}
		ByteBuffer result = (ByteBuffer) networkBuffers.getFirst();
		if (size == 1) {
			return result;
		}
		// ��ڂ̃o�b�t�@��put���[�h�ɂ��ėp��
		if (result.capacity() < packetSize) {
			result = PoolManager.getBufferInstance(packetSize);
			networkBuffers.addFirst(result);
			// �傫���o�b�t�@��p�ӂ��邽�߁A�P�߂͕K���z�������B
		} else {
			result.compact();//compact�͊댯����duplicate����邱�Ƃ��Ȃ��̂�OK
		}
		Iterator itr = networkBuffers.iterator();
		itr.next();// �P�ڂ�ǂݎ̂Ă�
		while (itr.hasNext()) {
			ByteBuffer byteBuffer = (ByteBuffer) itr.next();
			// byteBuffer.compact();
			int resultRemaining = result.remaining();
			int remaining = byteBuffer.remaining();
			if (resultRemaining >= remaining) {
				result.put(byteBuffer);
				itr.remove();// ���ׂċz�������̂ł���byteBuffer�́A����Ȃ�
				PoolManager.poolBufferInstance(byteBuffer);
			} else {
				int pos = byteBuffer.position();
				result.put(byteBuffer.array(), pos, resultRemaining);
				pos += resultRemaining;
				byteBuffer.position(pos);
				break;
			}
		}
		result.flip();
		return result;
	}

	private ByteBuffer unwrap(ByteBuffer src) throws SSLException {
		logger.debug("unwrap");
		ByteBuffer dst = PoolManager.getBufferInstance(packetSize);
		int srcremain=src.remaining();
		try {
			sslResult = sslEngine.unwrap(src, dst);
		} catch (SSLException e) {
			logger.warn("unwrap error.sslResult:"+sslResult + " srcremain:"+srcremain +" packetSize:"+packetSize);
			PoolManager.poolBufferInstance(dst);
			throw e;//dst�̘R���h������
		}
		if (src.remaining() == 0) {// src�o�b�t�@���g���؂�����networkBuffers���폜
			networkBuffers.remove(src);
			PoolManager.poolBufferInstance(src);
		}
		// logger.debug("unwrap:"+sslResult.getStatus() +
		// ":"+sslResult.getHandshakeStatus()+":"+src);
		HandshakeStatus handshakeStatus = sslResult.getHandshakeStatus();
		Status sslStatus = sslResult.getStatus();
		if (handshakeStatus == HandshakeStatus.NEED_TASK) {
			Runnable r = sslEngine.getDelegatedTask();
			r.run();
		}
		if (sslStatus == Status.BUFFER_UNDERFLOW) {
			PoolManager.poolBufferInstance(dst);
			return null;// ��M�l�b�g���[�N�f�[�^���s�����Ă���
		}
		if (sslStatus == Status.CLOSED) {
			PoolManager.poolBufferInstance(dst);
			return null;// �����܂�
		}
		if (sslStatus != Status.OK) {
			PoolManager.poolBufferInstance(dst);
			throw new RuntimeException("unwrap error.sslStatus:" + sslStatus);
		}
		dst.flip();
		return dst;
	}

	private ByteBuffer wrap(ByteBuffer[] buffers) throws SSLException {
		logger.debug("wrap");
		ByteBuffer dst = PoolManager.getBufferInstance(packetSize);
		sslResult = sslEngine.wrap(buffers, dst);
		// logger.info("wrap:"+sslResult.getStatus() +
		// ":"+sslResult.getHandshakeStatus());
		dst.flip();
		return dst;
	}

	private boolean handshake() throws SSLException {
		logger.debug("nextHandshake");
		HandshakeStatus handshakeStatus = sslResult.getHandshakeStatus();
		while (handshakeStatus != HandshakeStatus.FINISHED
				&& handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) {
			if (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
				if (networkBuffers.size() == 0) {
					handler.asyncRead(SSLCTX_READ_NETWORK);
					return false;
				}
				ByteBuffer src = nextUnwrapBuffer();
				ByteBuffer dst = unwrap(src);
				if (dst == null) {
					Status sslStatus = sslResult.getStatus();
					if (sslStatus == Status.BUFFER_UNDERFLOW) {
						handler.asyncRead(SSLCTX_READ_NETWORK);
					}
					// logger.error("unwrap error");
					return false;
				} else {
					// handshake����unwrap���ʂ̕K�v�Ȃ�
					PoolManager.poolBufferInstance(dst);
				}
			} else {
				asyncWrite(SSLCTX_PLAIN_DATA, dmmyBuffers);
			}
			handshakeStatus = sslResult.getHandshakeStatus();
		}
		isHandshake = true;
		return true;
	}

	/**
	 * channelHanndler��asyncWrite����f���Q�[�g�Ăяo�������B
	 * �c�O�Ȃ���A���[�U���w�肵��userContext���ɂ͋����ĖႤ//TODO
	 * 
	 * @param userContext
	 * @param timeout
	 * @param buffers
	 * @return
	 * @throws SSLException
	 */
	public boolean asyncWrite(Object userContext, ByteBuffer[] buffers)
			throws SSLException {
		synchronized (lock) {
			if (sslEngine == null) {
				// close���asyncWrite���ꂽ�ꍇ�̃_�~�[�������H�K�v���s��
				networkWriteCounter++;
				logger.debug("asyncWrite1 cid:"+handler.getChannelId() +":networkWriteCounter:"+networkWriteCounter);
				putOnWrittenMap(networkWriteCounter, userContext);
				return handler.asyncWrite(SSLCTX_WRITE_NETWORK, dmmyBuffers);
			}
			if (logger.isDebugEnabled()) {
				// ByteBuffer buffer=buffers[0];
				// logger.debug("asyncWrite."+new
				// String(buffer.array(),0,buffer.limit()));
			}
			boolean result = false;
			try {
				while (true) {
					ByteBuffer sslBuffer = wrap(buffers);
					if (sslBuffer == null) {
//2010/06/01			return false;
						break;
					}
					if (!sslBuffer.hasRemaining()) {
						PoolManager.poolBufferInstance(sslBuffer);
						// ���̏ꍇ�́Abuffers:dmmyBuffers
						break;
					}
					networkWriteCounter++;
					logger.debug("asyncWrite2 cid:"+handler.getChannelId() +":networkWriteCounter:"+networkWriteCounter);
					if (handler.asyncWrite(SSLCTX_WRITE_NETWORK, BuffersUtil.toByteBufferArray(sslBuffer))) {
						result = true;
					}
					if (BuffersUtil.remaining(buffers) == 0) {
						break;
					}
				}
			} finally {
				if (buffers != dmmyBuffers) {
					PoolManager.poolBufferInstance(buffers);
				}
			}
			//�o�^������O��write�����������Ⴄ�ꍇ������B
			putOnWrittenMap(networkWriteCounter, userContext);
			return result;
		}
	}

	/* SSL�ʐM���l������close�v���������Ȃ� */
	public boolean asyncClose(Object userContext) {
		logger.debug("asyncClose cid:"+handler.getChannelId() +":userContext:"+userContext);
		closeOutbound();
		if (handler != null) {
			//closeOutbound()��write����,����response��read���ׂ�
			//asyncRead(�I���v���g�R��)���āA������󂯎��Ύ��N���[�Y...���悢��
			//�A�v������asyncRead�����ɔ��s����Ă����獢��
			//�A�v���́AasyncRead��callback��҂���,asyncClose���Ă�...���肦��
			//asyncRead���Ȃ�AasyncRead(�I���v���g�R��)
			//asyncRead�ς݂Ȃ�asyncClose();
			//TODO userContext���A�v���ɒʒm������
			if(handler.asyncRead(SSLCTX_CLOSE_NETWORK)){
				logger.debug("asyncClose -> asyncRead OK cid:"+handler.getChannelId());
			}else{
				logger.debug("asyncClose -> asyncRead NG cid:"+handler.getChannelId());
			}
			return handler.asyncClose(SSLCTX_CLOSE_NETWORK);
		}
		return false;
	}

	/**
	 * SslHandler��onRead����f���Q�[�g�Ăяo�������B �c�O�Ȃ���A���[�U���w�肵��userContext���ɂ͋����ĖႤ//TODO
	 * 
	 * @param userContext
	 * @param buffers
	 * @throws SSLException
	 */
	public void onRead(Object userContext, ByteBuffer[] buffers)
			throws SSLException {
		for (int i = 0; i < buffers.length; i++) {
			networkBuffers.add(buffers[i]);
		}
		PoolManager.poolArrayInstance(buffers);
		// handshake��Ƀf�[�^��v�����Ȃ��ꍇ�Afalse�ƂȂ�B
		if (isHandshake == false) {
			if (sslResult == null) {// wrap��unwarp���Ă�ł��Ȃ���,handshake���I����ĂȂ�=>Server�̍ŏ��̏���
				ByteBuffer src = nextUnwrapBuffer();
				ByteBuffer dst = unwrap(src);
				// handshake����unwrap���ʂ̕K�v�Ȃ�;
				if (dst != null) {
					PoolManager.poolBufferInstance(dst);
				}
			}
			if (handshake() == false) {
				return;
			}
			if (handler.onHandshaked() == false) {
				// ���̃f�[�^���܂��K�v�Ȃ��ꍇ�͂��̂܂ܕ��A
				return;
			}
		}

		ArrayList list = null;
		while (true) {
			ByteBuffer src = nextUnwrapBuffer();
			if (src == null) {
				break;
			}
			ByteBuffer byteBuffer = unwrap(src);
			if (byteBuffer == null) {
				break;
			}
			if (list == null) {
				list = new ArrayList();
			}
			list.add(byteBuffer);
		}
		if (list == null) {
			// ���r���[�ȃf�[�^�����Ȃ��̂�����A���̃f�[�^��ǂݍ���
			logger.debug("unwrap return null.cid:" + handler.getChannelId());
			handler.asyncRead(userContext);
			return;
		}
		ByteBuffer[] plainBuffers = (ByteBuffer[]) list.toArray(BuffersUtil
				.newByteBufferArray(list.size()));
		// TODO readTrace����
		// handler.onRead(SSLCTX_PLAIN_DATA, plainBuffers);
		handler.callbackReadPlain(userContext, plainBuffers);
	}

	private long onWrittenCounter = 0;
	private long networkWriteCounter = 0;
	private Map<Long, Object> onWrittenMap = new HashMap<Long, Object>();

	private void putOnWrittenMap(long counter, Object userContext) {
		if (userContext == SSLCTX_PLAIN_DATA
				|| userContext == SSLCTX_WRITE_NETWORK) {
			logger.debug("putOnWrittenMap not put cid:" + handler.getChannelId()
					+ ":userContext:" + userContext + ":onWrittenCounter:" + counter);
			return;
		}
		if (userContext == null) {
			userContext = SSLCTX_DUMMY_USER_CONTEXT;
		}
		logger.debug("putOnWrittenMap cid:" + handler.getChannelId()
				+ ":userContext:" + userContext + ":onWrittenCounter:" + counter);
		onWrittenMap.put(counter, userContext);
	}

	public void onWritten(Object userContext) {
		synchronized (lock) {
			if(networkWriteCounter<=onWrittenCounter){//�܂�SSL�ʐM���n�߂�O��write��������������
				logger.warn("onWritten illegal context counter networkWriteCounter:"+networkWriteCounter +":onWrittenCounter:"+onWrittenCounter);
				handler.onWrittenPlain(userContext);
				return;
			}
			onWrittenCounter++;
			userContext = onWrittenMap.remove(onWrittenCounter);
			logger.debug("onWritten cid:" + handler.getChannelId()
				+ ":userContext:" + userContext + ":onWrittenCounter:"
				+ onWrittenCounter);
		}
		//�����܂�lock�ɓ����Ă������AonWrittenPlain�̉����ŁAWebServerHandler��lock���擾���ăf�b�g���b�N
		if (userContext != null) {
			if (userContext == SSLCTX_DUMMY_USER_CONTEXT) {
				handler.onWrittenPlain(null);
			} else {
				handler.onWrittenPlain(userContext);
			}
		}
	}

	//forward���ɖ���M��onWrtten�͎�M�������ɂ���
	public void forwardHandler(SslHandler handler) {
		logger.debug("forwardHandler cid:"+handler.getChannelId()+":onWrittenCounter:"+onWrittenCounter+":networkWriteCounter:"+networkWriteCounter);
		setHandler(handler);
		synchronized (lock) {
			onWrittenCounter=networkWriteCounter;
		}
	}
	
	public boolean isNetworkBuffer() {
		int size = networkBuffers.size();
		if (size == 0) {
			return false;
		}
		return true;
	}

	/*
	 * void print(){ SSLSession session=sslEngine.getSession();
	 * logger.info("getCipherSuite" + session.getCipherSuite()); try {
	 * Certificate[] pccs =session.getPeerCertificates(); for(int i=0;i<pccs.length;i++){
	 * logger.info("getPeerCertificates" + pccs[i]); } } catch
	 * (SSLPeerUnverifiedException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } Certificate[] pccs
	 * =session.getLocalCertificates(); for(int i=0;i<pccs.length;i++){
	 * logger.info("getLocalCertificates" + pccs[i]); } }
	 */
}
