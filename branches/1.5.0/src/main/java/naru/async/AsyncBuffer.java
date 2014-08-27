package naru.async;

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
	public boolean asyncBuffer(BufferGetter bufferGetter,Object userContext);
	
	/**
	 * buffer����postion�������Ȃ����߁A���x���ǂ݂��������ł���
	 * @param bufferGetter
	 * @param offset
	 * @param userContext�@�C�ӂ̃I�u�W�F�N�g,bufferGetter�C���^�t�F�[�X��callback���\�b�h�ɒʒm�����
	 * @return �v�����󂯕t����ꂽ�ꍇ,true
	 */
	public boolean asyncBuffer(BufferGetter bufferGetter,long offset,Object userContext);
	
	/**
	 * @return buffer���ێ�����f�[�^����ԋp�A�s���ȏꍇ�͕��̒l��ԋp����
	 */
	public long bufferLength();
}
