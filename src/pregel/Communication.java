package pregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Communication {
	//可以从这个类得到入边的信息
	//重置出边的信息，即改写入边
	//static int vertexNum = 4;
	static ArrayList<String> vertex = new ArrayList<>();//所有的点
	static String messagePerVertex[];
	static Map<String, Double> value = new HashMap<String, Double>();//存点上的值
	static Map<String, String> active = new HashMap<String, String>();//点是否活跃
	static Map<String, String> ifRead = new HashMap<String, String>();//该点的内容是否已经被读出来了
	static Map<String, String> ifModify = new HashMap<String, String>();
	
	public boolean isAllNotActive(){
		for(String s :vertex) {
			if(active.get(s).equals("Y")) {
				return false;//还没结束
			}
		}
		return true;
	}
	
	public void getvertexList(ArrayList<String> vertexList){
		/*
		for(String v[]:vertexList) {//每个区域
			for(String s :v) {//每个点
				vertex.add(s);
			}
		}*/
		vertex  = vertexList;
	}
	
	public void initeMessageList() {
		//初始化交流列表
		System.out.println("点集的大小："+vertex.size());
		messagePerVertex = new String[vertex.size()];
		for(int i=0;i<vertex.size();i++) {
			messagePerVertex[i]="";
		}
		//初始化value
		value.put("0", (double) 3);
		value.put("1", (double) 6);
		value.put("2", (double) 2);
		value.put("3", (double) 1);
		//初始化活跃点
		for(String s :vertex) {
			active.put(s, "Y");
			ifRead.put(s, "N");
			ifModify.put(s, "N");
		}
	}
	
	public void setMessage(String start_vertex, String dest_vertex, Double message) {
		int vertexId = Integer.valueOf(dest_vertex);
		String s = messagePerVertex[vertexId];
		//System.out.println("插入："+start_vertex+" "+message);
		s = s+start_vertex+" "+message+"|";//形式：<"0 3"|"2 2"|>
		messagePerVertex[vertexId] = s;//改写某个点接受的信息
	}
	
	public String[] getMessage() {
		return messagePerVertex;
	}
	
	public void clearVertexMessage(String id) {
		//清空某行的信息
		messagePerVertex[Integer.valueOf(id)] = "";
		ifModify.put(id, "Y");//改行被改动过
	}
	
	public void clearIfRead() {
		for(String s :vertex) {
			ifRead.put(s, "N");
			ifModify.put(s, "N");
		}
	}
}
