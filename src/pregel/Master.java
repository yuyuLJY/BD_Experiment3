package pregel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Master implements Runnable{
	
	//master�Ȱ��ձ߽��зָ�ѷָ������������������һ��2��worker����������txt
	//��ȡ�ָ�����������ÿ��worker���ָ�����
	//���Ƴ����Ľ���
	static ArrayList<String> vertexAllList = new ArrayList<>();
	static ArrayList<String> edgeAllList = new ArrayList<String>();
	static ArrayList<String[]> vertexList = new ArrayList<>();//����i�ĵ�����<[0,2]><[1,3]>
	static ArrayList<String[]> edgeList = new ArrayList<String[]>();//����i�ıߵ����<["1 0"]["0 1"]><>
	static int workerNum = 2;//worker�ĸ���
	int currentWorkId;
	static boolean finishmentStatus[] = new boolean[workerNum];
	//static Map<String, String> value = new HashMap<String, String>();//����ϵ�ֵ
	public void partition(ArrayList<String> vList,ArrayList<String> eList){
		/**
		 * ʹ��  ���Խ��л���
		 * @param vList�����б�
		 * @param eList���б�����ÿһ���ߴ���һ���������ʽ��[���ߣ����]
		 * @return �ѽ��д������(txt)
		 */
		//Step1:�ȶԵ���л���
		//�ж������ߣ����ж��ٸ�StringԪ��,ÿ��String��¼�˸�worker������Щ��
		//<"3 1","0 2">
		ArrayList<String> vertexPerWorker = new ArrayList<>();
		for(int i=0;i<workerNum;i++) {//�ȳ�ʼ��װworker�ĵ��String
			vertexPerWorker.add("");
		}
		//TODO ��֤�Ƿ��������ô��e.g��2
		System.out.println("vertex�Ĵ�С��"+vertexPerWorker.size());
		
		for(int i=0;i<vList.size();i++) {//�������б�
			int workerId = Integer.valueOf(vList.get(i))%workerNum;
			String s = vertexPerWorker.get(workerId);//ȡ��ԭ����������ʲô
			s = s+vList.get(i)+" ";//������µĵ�
			vertexPerWorker.set(workerId, s);
		}
		//TODO ��֤�Ƿ��¼��ÿ��worker�ĵ㶼��ȷ
		for(int i = 0;i<vertexPerWorker.size();i++) {
			System.out.println("����"+i+"�ĵ� "+vertexPerWorker.get(i));
		}
		
		//Step2������ÿ��worker�����еıߣ����ÿ������Ӧ�ö�Ӧ�ı�
		//<"1 3|2 3|2 1|","1 0|3 2|2 1|">
		ArrayList<String> edgePerWorker = new ArrayList<>();
		for(int i=0;i<workerNum;i++) {//�ȳ�ʼ��װworker�ĵ��String
			edgePerWorker.add("");
		}
		
		for(int i = 0;i<vertexPerWorker.size();i++) {
			//ȡ������i�ı�String
			String edgeWorkerI = edgePerWorker.get(i);
			
			//�鿴����i����Щ��
			String split[] = vertexPerWorker.get(i).split(" ");
			for(int j=0;j<split.length;j++) {//����������ĵ�
				//�ѱ���ӽ���
				String vertex = split[j];
				//System.out.println("����"+i+"�ĵ㣺"+vertex);
				for(String e :eList) {
					//�ñ߰��������ڵĵ㣬ͬʱ�������߻�û����ӽ������edgeString
					//System.out.println("�ߣ�"+e);
					if((e.contains(vertex)) && (!edgeWorkerI.contains(e))) {
						edgeWorkerI = edgeWorkerI+e+"|";
					}
				}
			}
			edgePerWorker.set(i, edgeWorkerI);
		}
		//TODO ��֤ÿ������ı��Ƿ������ȷ
		for(int i = 0;i<edgePerWorker.size();i++) {
			System.out.println("����"+i+"�ı� "+edgePerWorker.get(i));
		}
		
		//����Щ��Ϣд��TXT
		for(int i=0;i<workerNum;i++) {
			writePartitionToTxt(vertexPerWorker.get(i),edgePerWorker.get(i),i);
		}

	}
	
	public void writePartitionToTxt(String vertex,String edge,int i) {
		/**
		 * ��ĳ������ĵ�ͱߵ���Ϣд��txt
		 * @param vertex ĳ��worker����ĵ�,��ʽ��<"3 1","0 2">
		 * @param edge ĳ��worker����ı�,��ʽ��<"1 3|2 3|2 1|","1 0|3 2|2 1|">
		 */
		//д��txt
        String outurl="E:\\A������\\�����ݷ���\\ʵ��\\ʵ��3\\max\\partition\\"+String.valueOf(i)+".txt";
        File outfile=new File(outurl);
        try {
            //���д����ļ������ڴ������ļ�
            if(!outfile.exists()){
                outfile.createNewFile();
            }
            //�ļ�����������ȡ�ļ�
            FileOutputStream out=new FileOutputStream(outfile);
            //д�ļ�
            BufferedWriter write=new BufferedWriter(new OutputStreamWriter(out));
            
            String temp="";
            //д���
            write.write(vertex+"\r\n");
            //д���
            write.write(edge+"\r\n");
            //String splitE[] = edge.split("\\|");
            //for(String s :splitE){
                //д���ļ�
                //write.write(s+"\r\n");
                //System.out.println("д��"+s);
            //}
            write.close();
            out.close();
        } catch (Exception e) {
            // TODO �Զ����ɵ� catch ��
            e.printStackTrace();
        }
		
	}
	
	public static void load(String fileFolder) {
		/**
		 * ��ͼ���ֵĽ��������
		 * ĳ��worker����ĵ�,��ʽ��<"3 1","0 2">
		 * ĳ��worker����ı�,��ʽ��<"1 3|2 3|2 1|","1 0|3 2|2 1|">
		 * @param fileFolder ����������partition��Ϣ���ļ���
		 */
		//������Ӧ�������ļ�
		for(int i=0;i<workerNum;i++) {
			try {
				FileReader fr = new FileReader(fileFolder+String.valueOf(i)+".txt");
				BufferedReader bf = new BufferedReader(fr);
				String str;
				// ���ж�ȡ�ַ���
				str = bf.readLine();//��һ�е���Ϣ����
				//�ָ��
				String v[] = str.split(" ");
				//Collections.addAll(vertexList,v);
				vertexList.add(v);
				while ((str = bf.readLine()) != null) {
					//��һ��һ�µ���Ϣ����
					//�ָ��
					String[] split = str.split("\\|");//�ָ����õ��ܶ�ı�
					edgeList.add(split);
				}
				bf.close();
				fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//��֤load�����ɵ�vertexList��edgeList�Ƿ���ȷ
		for(int i=0;i<vertexList.size();i++) {
			System.out.println("����"+i+"�ĵ� "+Arrays.toString(vertexList.get(i)));
		}
		for(int i=0;i<edgeList.size();i++) {
			System.out.println("����"+i+"�ı� "+Arrays.toString(edgeList.get(i)));
		}
		//ǿ�е���ֵ
	}
	
	
	public static void masterInit() {
		//��ʼ��communication��
		Communication c = new Communication();
		//����communication����Щ��
		c.getvertexList(vertexAllList);
		System.out.println("ִ��initeMessageList");
		c.initeMessageList();//��ʼ������������Ϣ
		for(int k=0;k<workerNum;k++) {//����ÿ������
			for(int i=0;i<vertexList.get(k).length;i++) {//������k�ĵ���б���
				String currentVertex = vertexList.get(k)[i];
				for(int j=0;j<edgeList.get(k).length;j++) {//������k�ı߽��б���
					String split[] = edgeList.get(k)[j].split(" ");
					if(split[0].equals(currentVertex)) {
						//System.out.println("����"+k+" ����"+currentVertex+" ��:"+Arrays.toString(split));
						c.setMessage(split[0],split[1], c.value.get(split[0]));
					}
				}
			}
		}
	}

	//�߳�ִ��
	public void run() {
		//ÿ��workerִ�еĶ���
		System.out.println("�߳����ƣ�"+Thread.currentThread().getName()+"worker"+currentWorkId);
		Worker w = new Worker(vertexList.get(currentWorkId),edgeList.get(currentWorkId));
		w.startWorker();//������Ϣ�б�
		while(w.WorkerFinishment()==false) {
			System.out.println("δ����:�߳�"+Thread.currentThread().getName());
		}
		//�߳̽�����
		finishmentStatus[Integer.valueOf(Thread.currentThread().getName())]=true;
		System.out.println("!!����:�߳�"+Thread.currentThread().getName());
		
	}
	public static void readGraph(String fileName){
		//���ж�ȡ
		ArrayList<String> arrayList = new ArrayList<>();
		try {
			FileReader fr = new FileReader(fileName);
			BufferedReader bf = new BufferedReader(fr);
			String str;
			// ���ж�ȡ�ַ���
			while ((str = bf.readLine()) != null) {
				String[] split = str.split("\t");//�ָ���
				//�����б�
				if(!vertexAllList.contains(split[0])) {
					vertexAllList.add(split[0]);
				}
				if(!vertexAllList.contains(split[1])) {
					vertexAllList.add(split[1]);
				}
				//�����б�
				edgeAllList.add(split[0]+" "+split[1]);
			}
			bf.close();
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void lookOutCurrentStatus() {
		Communication c = new Communication();
		System.out.println("==============״̬============");
		String m[] = c.getMessage();
		for(int i=0;i<m.length;i++) {
			System.out.println("��"+i+"�� "+m[i]);
		}
		System.out.println("��֤value");
		System.out.println("��֤active");
		System.out.println("��֤ifRead");
		for(String s :c.vertex) {
			System.out.println("��"+s+" value "+c.value.get(s));
		}
		for(String s :c.vertex) {
			System.out.println("��"+s+" ifRead "+c.ifRead.get(s));
		}
		for(String s :c.vertex) {
			System.out.println("��"+s+" active "+c.active.get(s));
		}
		System.out.println("==========================");
	}
	
	public void setCurrentWorkId(int i) {
		this.currentWorkId = i;
		System.out.println("currentWorkId��ֵ��"+currentWorkId);
	}
	
	public static boolean isAllWorkerFinish() {
		for(boolean s : finishmentStatus) {
			if(s==false) {
				return false;
			}
		}
		return true;
	}
	
	public static void main() throws InterruptedException {
		//Step1:��graph�ж�ȡ�ߵĹ�ϵ����master���зָ�洢ÿ������ıߺ͵����Ϣ
		String fileName = "E:\\A������\\�����ݷ���\\ʵ��\\ʵ��3\\max\\graph.txt";
		readGraph(fileName);
		load("E:\\A������\\�����ݷ���\\ʵ��\\ʵ��3\\max\\partition\\");
		
		//Step2:��ʼ��communicate�б�
		masterInit();//��ʼ��
		//TODO ��֤��ʼ������Ϣ�Ƿ���ȷ
		lookOutCurrentStatus();
		
		
		//Step3:��ÿ������������֪worker,���߳�
		//TODO ����һ�ֳ�����Ҫ��ɶ���
		//���������߳�
		
		Communication c = new Communication();
		int stepNum =0;
		while(stepNum<8 && (!c.isAllNotActive())) {
			//����
			for(int i=0;i<finishmentStatus.length;i++) {
				finishmentStatus[i] = false;
			}
			System.out.println("===================������"+stepNum+"==========================");
			stepNum++;
			for(int i=0;i<workerNum;i++) {
				Master master = new Master();
				master.setCurrentWorkId(i);
				Thread t = new Thread(master,String.valueOf(i));
				t.start();
			}
			
			//Step4:����ͬ�������ж����еĽ��̶�ִ�����
			while(isAllWorkerFinish()==false) {
				//System.out.println("����workerδ����");
			}
			System.out.println("���е��̶߳�������");
			
			c.clearIfRead();//��һ�ֿ�ʼǰ��׼��
			//Step5����鵱�����
			System.out.println("Worker��ִ�����");
			lookOutCurrentStatus();
		}
	}
}
