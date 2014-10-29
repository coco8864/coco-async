package naru.async.store2;
/*
 * Store - Page
 *          |
 *         PagePool - Storage
 * 
 */


/**
 * �P��ɕԋp����T�C�Y�́ABuffer�������\�I�ɍœK�Ȃ悤�ɕԂ�
 * @author Owner
 *
 */
public interface AsyncBuffer {
	/**
	 * buffer����postion�����������Ăяo���ꍇ�Ɏ���
	 * �ǂݏo���͈�񂫂�ƂȂ�
	 * @param bufferGetter
	 * @param userContext �C�ӂ̃I�u�W�F�N�g,bufferGetter�C���^�t�F�[�X��callback���\�b�h�ɒʒm�����
	 * @return �v�����󂯕t����ꂽ�ꍇ,true
	 */
	public boolean startBuffer(BufferGetter bufferGetter,Object userContext);
	
	public boolean nextBuffer();
	
	public boolean endBuffer();
	
	/**
	 * @return buffer���ێ�����f�[�^����ԋp�A�s���ȏꍇ�͕��̒l��ԋp����
	 */
	public long bufferLength();
}
