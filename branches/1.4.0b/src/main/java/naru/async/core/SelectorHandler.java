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
import naru.async.core.SelectOperator.State;
import naru.async.pool.PoolManager;

public class SelectorHandler implements Runnable {
	static private Logger logger=Logger.getLogger(SelectorHandler.class);
	private int id;
	
	private long selectInterval;
	private SelectorStastics stastics;
	private Selector selector;
	
	private Thread selectorThread;
	private LinkedList<ChannelContext> contexts=new LinkedList<ChannelContext>();
	private boolean isWakeup;
	private boolean isRun;
	
	public int getId(){
		return id;
	}
	SelectorHandler(int id,long selectInterval) throws IOException{
		this.id=id;
		this.selectInterval=selectInterval;
		this.selector = Selector.open();
		this.stastics=new SelectorStastics(id,selectInterval);
	}
	
	SelectorStastics getStastics(){
		return stastics;
	}
	
	public void queueSelect(ChannelContext context){
		logger.debug("queueSelect.cid:"+context.getPoolId());
		synchronized(contexts){
			stastics.inQueue();
			context.ref();
			contexts.add(context);
		}
		wakeup();
	}
	public void wakeup(){
		if(isWakeup==false){
			//複数スレッドから同時に呼び出された場合、selector.wakeup()が複数呼び出される
			//可能性があるが容認（synchronizedしてまで守る必要なし)
			isWakeup=true;
			selector.wakeup();
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
	 * @param nextWakeup 次のタイムアウト時刻で一番近い時刻
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
			context.getSelectOperator().queueIo(State.closing);
			context.cancelSelect();
		}
	}
	
	private long checkIn(long timeoutTime,Set<ChannelContext> selectIn){
		//Set<ChannelContext> nextContexts=new HashSet<ChannelContext>();
		while(true){
			ChannelContext context;
			synchronized(contexts){
				if(contexts.size()==0){
					break;
				}
				context=(ChannelContext)contexts.remove(0);
			}
			if(context.select()){
				long time=context.getNextSelectWakeUp();
				if(time<timeoutTime){
					timeoutTime=time;
				}
				selectIn.add(context);
			}else{//参加したかったが、参加させられなかった。
				logger.debug("selectAdd fail to add.cid:"+context.getPoolId());
				//nextContexts.add(context);
			}
			context.unref();
		}
		//for(ChannelContext context:nextContexts){
		//	queueSelect(context);
		//}
		return timeoutTime;
	}
	
	private void accept(ChannelContext context,ServerSocketChannel serverSocketChannel){
		SocketChannel socketChannel;
		try {
			socketChannel = serverSocketChannel.accept();
			if(socketChannel==null){
				return;
			}
			Socket socket=socketChannel.socket();
			if( !context.acceptable(socket) ){
				logger.debug("refuse socketChannel:"+socketChannel);
				socket.close();//接続拒否
				stastics.acceptRefuse();
				return;
			}
			logger.debug("isAcceptable socketChannel:"+socketChannel);
		} catch (IOException e) {
			logger.error("fail to accept.",e);
			return;
		}
		//ユーザオブジェクトを獲得する
		ChannelHandler handler=(ChannelHandler)PoolManager.getInstance(context.getAcceptClass());
		ChannelContext acceptContext=ChannelContext.socketChannelCreate(handler, socketChannel);
		Object userAcceptContext=context.getAcceptUserContext();
		acceptContext.accepted(userAcceptContext);
		stastics.read();
		acceptContext.getSelectOperator().queueSelect(State.selectReading);
	}
	
	private boolean dispatch(SelectionKey key,ChannelContext context){
		if(key.isWritable()){
			stastics.write();
			context.getWriteOperator().writable();
		}
		if(key.isConnectable()){
			stastics.connect();
			context.getSelectOperator().connectable();
			return true;
		}else if(key.isReadable()){
			stastics.read();
			context.getSelectOperator().readable();
			return true;
		}else if(key.isAcceptable()){
			SelectableChannel selectableChannel=key.channel();
			ServerSocketChannel serverSocketChannel =(ServerSocketChannel) selectableChannel;
			accept(context, serverSocketChannel);
		}
		return false;
	}
	
	private long checkOut(long timeoutTime,Set<ChannelContext> selectOut){
		Set<SelectionKey> keys=selector.keys();
		Iterator<SelectionKey> itr=keys.iterator();
		Set<SelectionKey> selectedKeys=selector.selectedKeys();
		while(itr.hasNext()){
			SelectionKey key=itr.next();
			if(key.isValid()==false){
				continue;
			}
			ChannelContext context=(ChannelContext)key.attachment();
			logger.debug("checkAll cid:"+context.getPoolId());
			if(selectedKeys.contains(key)){
				logger.debug("checkAll selectedKeys cid:"+context.getPoolId());
				if(dispatch(key, context)){
					//dispatchに成功したなら、selectを続ける必要なし
					context.ref();
					selectOut.add(context);
				}
			}else if(context.select()){
				logger.debug("checkAll context.select() cid:"+context.getPoolId());
				long time=context.getNextSelectWakeUp();
				if(time<timeoutTime){
					timeoutTime=time;
				}
				logger.debug("after select getNextSelectWakeUp.cid:"+context.getPoolId()+":"+(timeoutTime-System.currentTimeMillis()));
			}else{//参加させられなかった。
				logger.debug("checkAll fail to add.cid:"+context.getPoolId());
				context.ref();
				selectOut.add(context);
			}
		}
		return timeoutTime;
	}

	private static long MIN_INTERVAL_TIME=10;
	private static int MIN_INTERVAL_COUNT=16;
	/**
	 * リクエスト待ち、このメソッドはブロックします。
	 * リクエスト到着は、ChannelEventに通知します。
	 * 
	 * @throws IOException
	 */
	private void waitForSelect() throws IOException {
		isRun=true;
		isWakeup=false;//若干余分にwakeupが呼び出されるのは許容する
		long nextWakeup=System.currentTimeMillis()+selectInterval;//次にtimeoutする直近時刻
		long lastWakeup=System.currentTimeMillis();
		int minCount=0;
		Set<ChannelContext> selectOut=new HashSet<ChannelContext>();
		Set<ChannelContext> selectIn=new HashSet<ChannelContext>();
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
			lastWakeup=System.currentTimeMillis();
//			logger.info("select in.this:"+this+":interval:"+interval);
			Set<SelectionKey> keys=selector.keys();
			stastics.setSelectCount(keys.size());
			if(interval>0){
				selectCount=selector.select(interval);
			}else{
				selectCount=selector.selectNow();
			}
			isWakeup=false;//排他してないので、若干余分にwakeupが呼び出されるのは許容する
			if(selectCount<0){
				logger.info("select out break.selectCount:"+selectCount+":this:"+this);
				break;
			}else if(selectCount!=0){
				minCount=0;//イベントが発生してwakeupされたならカウンタをクリア
			}
			/* 選択されたチャネルを処理する */
			nextWakeup=System.currentTimeMillis()+selectInterval;
			nextWakeup=checkOut(nextWakeup,selectOut);
			nextWakeup=checkIn(nextWakeup,selectIn);
			/* selectOutの後、selectInしていないkeyをcancelする */
			Iterator<ChannelContext> outItr=selectOut.iterator();
			while(outItr.hasNext()){
				ChannelContext out=outItr.next();
				if(!selectIn.contains(out)){
					out.cancelSelect();
				}
				out.unref();
				outItr.remove();
			}
			selectIn.clear();
			logger.debug("nextWakeup:"+nextWakeup+":"+(nextWakeup-System.currentTimeMillis()));
			
			/* 停止要求されている場合は、停止 */
			if( isRun==false ){
				closeAll();
				selector.close();
				break;
			}
		}
	}
	
	public void run() {
		try {
			waitForSelect();
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
