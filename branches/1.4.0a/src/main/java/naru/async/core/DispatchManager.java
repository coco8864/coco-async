package naru.async.core;

import java.util.Map;

import org.apache.log4j.Logger;

import naru.queuelet.Queuelet;
import naru.queuelet.QueueletContext;

public class DispatchManager implements Queuelet {
	private static Logger logger=Logger.getLogger(DispatchManager.class);
	private static QueueletContext queueletContext;
	/**
	 * queuelet�@terminal�ւ̃L���[�C���O
	 * @author Naru
	 *
	 */
	public static void enqueue(Object obj){
		if(obj==null){
			return;
		}
		queueletContext.enque(obj);
	}
	
	public void init(QueueletContext context, Map param) {
		logger.info("DispatchManager init");
		queueletContext=context;
	}
	
	//async�̃��W���[���̒��ň�ԏ��߂�term���󂯕t����Ƃ���
	public void term() {
		logger.info("DispatchManager term");
		ChannelContext.dumpChannelContexts(logger);
	}

	public boolean service(Object req) {
		ChannelContext channelContext=(ChannelContext)req;
		channelContext.callback();
		return true;
	}
}
