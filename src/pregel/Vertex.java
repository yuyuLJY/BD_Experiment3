package pregel;

public abstract class Vertex {
	String vertexValue;//����ĵ�ǰֵ
	int vertexId;//����ID string
	String EdgeValue;//������˭
	String MessageValue;//��Ϣ
	int superstep;//������ int64
	String GetValue;//������ص�ֵ VertexValue
	int isActive=1;//�Ƿ��Ծ
	public abstract double Compute(String s,String vid);
	
	public String[] GetEdge() {
		//���ܱ���Ϣ��ÿ������߰�����Ŀ�궥��ID�ͱߵ�ֵ
		Communication c = new Communication();
		String s[] = c.getMessage();
		return s;
	}
	
	public void SendMessageTo(String start_vertex,String dest_vertex, Double message) {
		//��������Ϣ��֪����˭��
		//���޸���Ϣ�б�
		Communication c = new Communication();
		c.setMessage(start_vertex, dest_vertex, message);
	}
	
	public void clearVertexMessage(String id) {
		Communication c = new Communication();
		c.clearVertexMessage(id);
	}
	
    void VoteToHalt() {
    	//��Ϊͣ��
    }
    
}
