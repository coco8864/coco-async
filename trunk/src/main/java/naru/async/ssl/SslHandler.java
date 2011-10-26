package naru.async.ssl;

import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;
/**
 * SSL���l�����邽�߂̐��`
 * �ȉ��AasyncRead�Ăяo���́A���̃N���X�������I�s��
 * 1)handshake���Ƀl�b�g���[�N�f�[�^���K�v�ȏꍇ
 * 2)�f�[�^�ŃR�[�h����BUFFER_UNDERFLOW�ƂȂ����ꍇ
 * 3)�T�[�o���[�h�ōŏ��̃��[�U�f�[�^��v�����邽�߂�asyncRead(!!!�d�v!!!)
 * 4)�T�[�o�A�N���C�A���g���[�h�ɂ�炸�AdoHandshake��true���A���A���l�b�g���[�N�f�[�^����M���݂łȂ������ꍇ��asyncRead(!!!�d�v!!!)
 * 
 * @author naru
 *
 */
public abstract class SslHandler extends ChannelHandler {
	private static Logger logger = Logger.getLogger(SslHandler.class);
	/* read��userContext�̈��p���p */
	private Object closeLock=new Object();
	private SslAdapter sslAdapter;
	private Store readPeekStore;
	private Store writePeekStore;
	
	public void recycle() {
		if(sslAdapter!=null){
			sslAdapter.unref(true);
			sslAdapter=null;
		}
		this.readPeekStore=null;
		this.writePeekStore=null;
		super.recycle();
	}

	public abstract SSLEngine getSSLEngine();
	
//	public void setSslAdapter(SslAdapter sslAdapter){
//		this.sslAdapter=sslAdapter;
//	}
	
	public ChannelHandler forwardHandler(SslHandler handler){
		SslAdapter sslAdapter=this.sslAdapter;
		Store readPeekStore=this.readPeekStore;
		Store writePeekStore=this.writePeekStore;
		this.sslAdapter=null;
		this.readPeekStore=null;
		this.writePeekStore=null;
		super.forwardHandler(handler);
		handler.sslAdapter=sslAdapter;
		handler.readPeekStore=readPeekStore;
		handler.writePeekStore=writePeekStore;
		if(sslAdapter!=null){
//			sslAdapter.setHandler(handler);
			sslAdapter.forwardHandler(handler);
		}
		return handler;
	}

	public ChannelHandler forwardHandler(Class handlerClass){
		SslAdapter sslAdapter=this.sslAdapter;
		Store readPeekStore=this.readPeekStore;
		Store writePeekStore=this.writePeekStore;
		this.sslAdapter=null;
		this.readPeekStore=null;
		this.writePeekStore=null;
		
		long cid=getChannelId();
		SslHandler handler= (SslHandler)super.forwardHandler(handlerClass);
		if(handler==null){//����close����Ă��铙��forward�Ɏ��s
			logger.debug("forwardHandler ng.cid:"+cid);
			this.sslAdapter=sslAdapter;
			this.readPeekStore=readPeekStore;
			this.writePeekStore=writePeekStore;
			return null;
		}
		handler.sslAdapter=sslAdapter;
		handler.readPeekStore=readPeekStore;
		handler.writePeekStore=writePeekStore;
		if(sslAdapter!=null){
			logger.debug("forwardHandler ok.cid:"+cid);
//			sslAdapter.setHandler(handler);
			sslAdapter.forwardHandler(handler);
		}
		return handler;
	}
	
	/**
	 * �ǂݍ��񂾕������������郁�\�b�h
	 * @param buffers
	 */
	public void onReadPlain(Object userContext,ByteBuffer[] buffers) {
		//�I�[�o���C�h���Ă�
		return;
	}
	
	/**
	 * asyncWrite�ɑ΂��銮���ʒm���\�b�h
	 * onWritten�́Assl�ŕ������ꂽ�������݂ɑΉ����邽�߂P�΂P�ɑΉ����Ȃ�
	 * @param buffers
	 */
	public void onWrittenPlain(Object userContext) {
		//�I�[�o���C�h���Ă�
		return;
	}
	
	public final void onWritten(Object userContext){
		logger.debug("onWritten cid:" + getChannelId() + ":userContext:"+userContext +":sslAdapter:"+sslAdapter);
		if(sslAdapter==null){
			onWrittenPlain(userContext);
			return;
		}
		sslAdapter.onWritten(userContext);
	}
	
	/**
	 * SSLhandshake������ɏI����������ɂ�т������A���\�b�h
	 * ���̃��\�b�h�̒���asyncRead���Ăяo���Ă͂����Ȃ�
	 * ���̑���A���A�l��true�̏ꍇ�ɂ́A�K�v�ɉ�����async����asyncRead���Ăяo��
	 * @return �l�b�g���[�N������̎��̃f�[�^���K�v�ȏꍇtrue
	 */
	public boolean onHandshaked() {
		//�I�[�o���C�h���Ă�
		return false;
	}
	
	public boolean isSsl(){
		return (sslAdapter!=null);
	}
	
	public boolean sslOpen(boolean isClientMode){
		if(sslAdapter!=null){
			logger.error("sslOpen aleady set sslAdapter");
			sslAdapter.unref();
		}
		sslAdapter=(SslAdapter) PoolManager.getInstance(SslAdapter.class);
		return sslAdapter.open(isClientMode, this);
	}
	
	public boolean sslOpenWithBuffer(boolean isClientMode,ByteBuffer[] buffers){
		if(sslAdapter!=null){
			logger.error("sslOpenWithBuffer aleady set sslAdapter");
			sslAdapter.unref();
		}
		sslAdapter=(SslAdapter) PoolManager.getInstance(SslAdapter.class);
		return sslAdapter.openWithBuffer(isClientMode, this, buffers);
	}
	
	/**
	 * �Ⴆ�΂̎���
	 */
	public void onAccepted(Object userContext) {
//		sslOpen(false,readTimeout,writeTimeout);
	}
	
	public void callbackReadPlain(Object userContext, ByteBuffer[] buffers){
		if(readPeekStore!=null){
			ByteBuffer[] peekBuffers=PoolManager.duplicateBuffers(buffers);;
			readPeekStore.putBuffer(peekBuffers);
		}
		onReadPlain(userContext,buffers);
	}
	
	/**
	 * �K�v������ꍇ�A����������
	 */
	public void onRead(Object userContext, ByteBuffer[] buffers) {
		if(sslAdapter==null){
			callbackReadPlain(userContext,buffers);
			return;
		}
		if(userContext==SslAdapter.SSLCTX_PLAIN_DATA){
			callbackReadPlain(userContext,buffers);
			return;
		}else if(userContext==SslAdapter.SSLCTX_CLOSE_NETWORK){
			PoolManager.poolBufferInstance(buffers);//�ǂݎ̂�
//			asyncClose(SslAdapter.SSLCTX_CLOSE_NETWORK);
			return;
		}
		try {
			/* ���̉����ł�����xonRead���Ăяo����� */
			sslAdapter.onRead(userContext, buffers);
		} catch (SSLException e) {
//			sslAdapter.print();
			onReadFailure(userContext, e);
			logger.error("read ssl error.cid:"+getChannelId(),e);
			//���������s���̏���
		}
	}
	
	//SSLEngine�ɂ܂��f�[�^���c���Ă��邩������Ȃ��B
	public void onClosed(Object userContext) {
		logger.debug("#closed.cid:" + getChannelId());
		if(sslAdapter!=null){
			/* ���̉�����onRead���Ăяo����邩������Ȃ� */
			sslAdapter.closeInbound();
		}
		super.onClosed(userContext);
	}

	public void onFinished() {
		logger.debug("#finished.cid:" + getChannelId() +":sslAdapter:" +sslAdapter);
		synchronized(closeLock){
			if(sslAdapter!=null){
				sslAdapter.unref(true);
				sslAdapter=null;
			}
		}
	}
	
	/**
	 * �K�v������ꍇ�A�Í������đ��M����悤�ɃI�[�o�[���C�h
	 */
	public boolean asyncWrite(Object userContext,ByteBuffer[] buffers){
		if(sslAdapter==null){
			//sslContext�z���ɂȂ��ꍇ�́A����ɒ��ړ�����
			if(writePeekStore!=null){
//				ByteBuffer[] peekBuffers=BuffersUtil.dupBuffers(buffers);
				ByteBuffer[] peekBuffers=PoolManager.duplicateBuffers(buffers);
				writePeekStore.putBuffer(peekBuffers);
			}
			logger.debug("asyncWrite1 cid:"+getChannelId());
			return super.asyncWrite(userContext, buffers);
		}
		if(userContext==SslAdapter.SSLCTX_WRITE_NETWORK){
			logger.debug("asyncWrite2 cid:"+getChannelId());
			//SslAdapter����Ăяo���ꂽ�ꍇ�́A����ɒ��ړ�����
			return super.asyncWrite(userContext, buffers);
		}
		
		if(writePeekStore!=null){
//			ByteBuffer[] peekBuffers=BuffersUtil.dupBuffers(buffers);
			ByteBuffer[] peekBuffers=PoolManager.duplicateBuffers(buffers);
			writePeekStore.putBuffer(peekBuffers);
		}
		try {
			return sslAdapter.asyncWrite(userContext, buffers);
		} catch (SSLException e) {
			logger.error("asyncWrite ssl error",e);
			asyncClose(null);
		}
		return true;
	}
	
	/**
	 * SSL�������l������asyncClose�𔭍s���郁�\�b�h
	 */
	public boolean asyncClose(Object userContext){
		if(sslAdapter==null){
			return super.asyncClose(userContext);
		}
		if(userContext==SslAdapter.SSLCTX_CLOSE_NETWORK){
			return super.asyncClose(userContext);
		}
		return sslAdapter.asyncClose(userContext);
	}
	
	/*----�ȍ~�v���g�R��peek�֘A----*/
	public void pushReadPeekStore(Store readPeekStore){
		this.readPeekStore=readPeekStore;
	}
	public void pushWritePeekStore(Store writePeekStore){
		this.writePeekStore=writePeekStore;
	}
	
	public Store popReadPeekStore(){
		Store readPeekStore=this.readPeekStore;
		this.readPeekStore=null;
		return readPeekStore;
	}
	public Store popWritePeekStore(){
		Store writePeekStore=this.writePeekStore;
		this.writePeekStore=null;
		return writePeekStore;
	}
}
