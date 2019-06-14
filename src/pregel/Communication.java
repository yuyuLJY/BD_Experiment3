package pregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Communication {
	//���Դ������õ���ߵ���Ϣ
	//���ó��ߵ���Ϣ������д���
	//static int vertexNum = 4;
	static ArrayList<String> vertex = new ArrayList<>();//���еĵ�
	static String messagePerVertex[];
	static Map<String, Double> value = new HashMap<String, Double>();//����ϵ�ֵ
	static Map<String, String> active = new HashMap<String, String>();//���Ƿ��Ծ
	static Map<String, String> ifRead = new HashMap<String, String>();//�õ�������Ƿ��Ѿ�����������
	static Map<String, String> ifModify = new HashMap<String, String>();
	
	public boolean isAllNotActive(){
		for(String s :vertex) {
			if(active.get(s).equals("Y")) {
				return false;//��û����
			}
		}
		return true;
	}
	
	public void getvertexList(ArrayList<String> vertexList){
		/*
		for(String v[]:vertexList) {//ÿ������
			for(String s :v) {//ÿ����
				vertex.add(s);
			}
		}*/
		vertex  = vertexList;
	}
	
	public void initeMessageList() {
		//��ʼ�������б�
		System.out.println("�㼯�Ĵ�С��"+vertex.size());
		messagePerVertex = new String[vertex.size()];
		for(int i=0;i<vertex.size();i++) {
			messagePerVertex[i]="";
		}
		//��ʼ��value
		value.put("0", (double) 3);
		value.put("1", (double) 6);
		value.put("2", (double) 2);
		value.put("3", (double) 1);
		//��ʼ����Ծ��
		for(String s :vertex) {
			active.put(s, "Y");
			ifRead.put(s, "N");
			ifModify.put(s, "N");
		}
	}
	
	public void setMessage(String start_vertex, String dest_vertex, Double message) {
		int vertexId = Integer.valueOf(dest_vertex);
		String s = messagePerVertex[vertexId];
		//System.out.println("���룺"+start_vertex+" "+message);
		s = s+start_vertex+" "+message+"|";//��ʽ��<"0 3"|"2 2"|>
		messagePerVertex[vertexId] = s;//��дĳ������ܵ���Ϣ
	}
	
	public String[] getMessage() {
		return messagePerVertex;
	}
	
	public void clearVertexMessage(String id) {
		//���ĳ�е���Ϣ
		messagePerVertex[Integer.valueOf(id)] = "";
		ifModify.put(id, "Y");//���б��Ķ���
	}
	
	public void clearIfRead() {
		for(String s :vertex) {
			ifRead.put(s, "N");
			ifModify.put(s, "N");
		}
	}
}
