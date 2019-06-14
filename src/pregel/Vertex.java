package pregel;

public abstract class Vertex {
	String vertexValue;//顶点的当前值
	int vertexId;//顶点ID string
	String EdgeValue;//出边有谁
	String MessageValue;//消息
	int superstep;//超步数 int64
	String GetValue;//顶点相关的值 VertexValue
	int isActive=1;//是否活跃
	public abstract double Compute(String s,String vid);
	
	public String[] GetEdge() {
		//接受边消息：每条出射边包含了目标顶点ID和边的值
		Communication c = new Communication();
		String s[] = c.getMessage();
		return s;
	}
	
	public void SendMessageTo(String start_vertex,String dest_vertex, Double message) {
		//发出边消息：知道向谁发
		//即修改消息列表
		Communication c = new Communication();
		c.setMessage(start_vertex, dest_vertex, message);
	}
	
	public void clearVertexMessage(String id) {
		Communication c = new Communication();
		c.clearVertexMessage(id);
	}
	
    void VoteToHalt() {
    	//设为停机
    }
    
}
