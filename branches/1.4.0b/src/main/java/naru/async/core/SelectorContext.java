package naru.async.core;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;

import naru.async.ChannelHandler;
import naru.async.pool.PoolManager;

public class SelectorContext implements Runnable {
	static private Logger logger=Logger.getLogger(SelectorContext.class);
	private int id;
	
	private long selectInterval;
	private SelectorStastics stastics;
	private Selector selector;
	
	private Thread selectorThread;
	private LinkedList contexts=new LinkedList();
	private boolean isWakeup;
	private boolean isRun;
	
	public int getId(){
		return id;
	}
	SelectorContext(int id,long selectInterval) throws IOException{
		this.id=id;
		this.selectInterval=selectInterval;
		this.selector = Selector.open();
		this.stastics=new SelectorStastics(id,selectInterval);
	}
	
	SelectorStastics getStastics(){
		return stastics;
	}
	
	public void queueSelect(ChannelContext context){
		synchronized(contexts){
			stastics.inQueue();
			context.ref();
			contexts.add(context);
		}
	}
	public void wakeup(){
		if(isWakeup==false){
			//�����X���b�h���瓯���ɌĂяo���ꂽ�ꍇ�Aselector.wakeup()�������Ăяo�����
			//�\�������邪�e�F�isynchronized���Ă܂Ŏ��K�v�Ȃ�)
			isWakeup=true;
			selector.wakeup();
//			logger.info("wakeup this:"+this);
		}
	}
	
	public SelectionKey keyFor(SelectableChannel channel){
		return channel.keyFor(selector);
	}
	
	public  SelectionKey register(SelectableChannel channel,int ops,Object att) throws ClosedChannelException{
		return channel.register(selector, ops, att);
	}
	
	/**
	 * 
	 * @param nextWakeup ���̃^�C���A�E�g�����ň�ԋ߂�����
	 * @return
	 */
	private void closeAll(){
		Iterator itr=selector.keys().iterator();
		while(itr.hasNext()){
			SelectionKey key=(SelectionKey)itr.next();
			if(key.isValid()==false){
				continue;
			}
			ChannelContext context=(ChannelContext)key.attachment();
			context.queueIO(ChannelContext.IO.CLOSEABLE);
		}
	}
	
	/**
	 * 
	 * @param nextWakeup ���̃^�C���A�E�g�����ň�ԋ߂�����
	 * @return
	 */
	private long selectAll(){
		long timeoutTime=Long.MAX_VALUE;
		Set nextContexts=new HashSet();
		Set<SelectionKey> keys=selector.keys();
		Iterator<SelectionKey> itr=keys.iterator();
		while(itr.hasNext()){
			SelectionKey key=itr.next();
			if(key.isValid()==false){
				continue;
			}
			ChannelContext context=(ChannelContext)key.attachment();
			if(context.select()){
				long time=context.getNextSelectWakeUp();
				if(time<timeoutTime){
					timeoutTime=time;
				}
			}else{//�Q���������������A�Q���������Ȃ������B
				nextContexts.add(context);
			}
		}
		while(true){
			ChannelContext context;
			synchronized(contexts){
				if(contexts.size()==0){
					break;
				}
				context=(ChannelContext)contexts.remove(0);
			}
			logger.debug("selectAll cid:"+context.getPoolId());
			if(nextContexts.contains(context)){
				//����select�œ���context��select�𕡐���ĂԂ̂�h���A���Q�͂Ȃ��Ƃ͎v����
				context.unref();
				continue;
			}
			if(context.select()){
				long time=context.getNextSelectWakeUp();
				if(time<timeoutTime){
					timeoutTime=time;
				}
				context.unref();
			}else{//�Q���������������A�Q���������Ȃ������B
				nextContexts.add(context);
			}
		}
		int nextContextsCount=nextContexts.size();
		if(nextContextsCount!=0){
			if(logger.isDebugEnabled()){
				logger.debug("retry select.size:"+nextContexts.size());
				Iterator citr=nextContexts.iterator();
				while(citr.hasNext()){
					ChannelContext context=(ChannelContext)citr.next();
					logger.debug("id:"+context.getPoolId());
				}
			}
			synchronized(contexts){
				contexts.addAll(nextContexts);
			}
			//��select�������ɖڊo�߂鎖������
			selector.wakeup();
//			logger.info("wakeup2 this:"+this);
			
		}
		return timeoutTime;
	}
	
	public static String ATTR_ACCEPTED_CONTEXT="naru.async.acceptedContext";
	
	private void dispatch() {
		// �Z���N�g���ꂽ SelectionKey�I�u�W�F�N�g���܂Ƃ߂Ď擾����
		Iterator keyIterator = selector.selectedKeys().iterator();
		while (keyIterator.hasNext()) {
			SelectionKey key = (SelectionKey) keyIterator.next();
			keyIterator.remove();//�����ƑI�����ꂽ�܂܂ɂȂ鎖��h��
			
			ChannelContext context=(ChannelContext)key.attachment();
			SelectableChannel selectableChannel=key.channel();
			SocketChannel socketChannel=null;
			if(selectableChannel instanceof SocketChannel){
				socketChannel=(SocketChannel)selectableChannel;
			}
			// �Z���N�g���ꂽ SelectionKey �̏�Ԃɉ����ď��������߂�
			if(!key.isValid()){
				logger.warn("dispatch key is aleardy canceled");
			}else if (key.isAcceptable()) {//��������java.nio.channels.CancelledKeyException����������H
				ServerSocketChannel serverSocketChannel =(ServerSocketChannel) selectableChannel;
				try {
					socketChannel = serverSocketChannel.accept();
					Socket s=socketChannel.socket();
					if( !context.acceptable() ){
						logger.debug("refuse socketChannel:"+socketChannel);
						s.close();//�ڑ�����
						stastics.acceptRefuse();
						continue;
					}
					logger.debug("isAcceptable socketChannel:"+socketChannel);
				} catch (IOException e) {
					logger.error("fail to accept.",e);
					continue;//�ǂ����悤���Ȃ�
				}
				//���[�U�I�u�W�F�N�g���l������
				ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(context.getAcceptClass());
				ChannelContext acceptContext=ChannelContext.create(handler, socketChannel);
				
				handler.setHandlerAttribute(ATTR_ACCEPTED_CONTEXT, acceptContext);
				Object userAcceptContext=context.getAcceptUserContext();
				acceptContext.accepted(userAcceptContext);
				//2013/12/8 acceptContext�Ƃ肠�����ǂ�ł݂�(for perfomance)
				//acceptContext.queueuSelect();
				stastics.read();
				//acceptContext.setIoStatus(ChannelContext.IO.SELECT);
				acceptContext.queueIO(ChannelContext.IO.READABLE);
			} else if (key.isReadable()) {//READ��D��I�ɔ��f
//				context.readable(true);
				stastics.read();
				context.getReadChannel().readable();
			}else if(key.isWritable()){
//				context.writable(true);
				stastics.write();
				context.getWriteChannel().writable();
			}else if(key.isConnectable()){
				// �ڑ��\�ɂȂ����ꍇ
				stastics.connect();
				context.getReadChannel().connectable();
			}
		}
	}

	private static long MIN_INTERVAL_TIME=10;
	private static int MIN_INTERVAL_COUNT=16;
	/**
	 * ���N�G�X�g�҂��A���̃��\�b�h�̓u���b�N���܂��B
	 * ���N�G�X�g�����́AChannelEvent�ɒʒm���܂��B
	 * 
	 * @throws IOException
	 */
	private void waitForRequest() throws IOException {
		isRun=true;
		isWakeup=false;//�኱�]����wakeup���Ăяo�����̂͋��e����
		long nextWakeup=System.currentTimeMillis()+selectInterval;//����timeout���钼�ߎ���
		//������wakeup�����ƐQ��d�g��,10ms�ȉ���16��loop...�`���[�j���O�v�f
		long lastWakeup=System.currentTimeMillis();
		int minCount=0;
		while (true) {
			long now=System.currentTimeMillis();
			if( (lastWakeup-now)<MIN_INTERVAL_TIME ){
				minCount++;
//				System.out.println("minCount:"+minCount);
				if(minCount>=MIN_INTERVAL_COUNT){
					try {
						stastics.sleep();
						Thread.sleep(MIN_INTERVAL_TIME);
					} catch (InterruptedException ignore) {
					}
					now=System.currentTimeMillis();
					minCount=0;
				}
			}else{
				minCount=0;
			}
			long interval=nextWakeup-now;
			if(interval>selectInterval){
				interval=selectInterval;
			}
			logger.debug("select in.interval:"+interval);
			stastics.loop();
			int selectCount=0;
			if(interval>0){
				lastWakeup=System.currentTimeMillis();
//				System.out.println(Thread.currentThread().getName() + ":interval:"+interval);
//				logger.info("select in.this:"+this+":interval:"+interval);
				Set<SelectionKey> keys=selector.keys();
				stastics.setSelectCount(keys.size());
				selectCount=selector.select(interval);
				isWakeup=false;//�r�����ĂȂ��̂ŁA�኱�]����wakeup���Ăяo�����̂͋��e����
//				System.out.println(Thread.currentThread().getName() + ":out:"+interval);
				if(selectCount<0){
					logger.info("select out break.selectCount:"+selectCount+":this:"+this);
					break;
				}else if(selectCount!=0){
					minCount=0;//�C�x���g����������wakeup���ꂽ�Ȃ�J�E���^���N���A
				}
			}
			/* �I�����ꂽ�`���l������������ */
//			logger.info("select out.selectCount:"+selectCount+":this:"+this);
			//nextWakeup=System.currentTimeMillis()+selectInterval;
			dispatch();
			
			/* select���[�h�𒲐�����(close timeout�@�ύX) */
			nextWakeup=selectAll();
			
			/* ��~�v������Ă���ꍇ�́A��~ */
			if( isRun==false ){
				closeAll();
				selector.close();
				break;
			}
		}
	}
	
	public void run() {
		try {
			waitForRequest();
			logger.info("SelectorContext nomal end.stastics:"+stastics.getLoopCount() +":"+ stastics.getSleepCount());
		} catch (IOException e) {
			logger.error("SelectorContext listener IOException end.",e);
		} catch (Throwable e) {
			logger.error("SelectorContext listener Throwable end.",e);
		}
	}
	
	public void start(int index){
		isRun=true;
		selectorThread=new Thread(this);
		selectorThread.setName("selector-" + index);
		selectorThread.start();
	}
	
	public void stop(){
		isRun=false;
		selector.wakeup();
		try {
			selectorThread.join();
		} catch (InterruptedException e) {
			logger.error("fail to join",e);
		}
	}
}
