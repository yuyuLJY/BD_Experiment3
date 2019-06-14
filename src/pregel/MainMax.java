package pregel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class MainMax {
	//TODO PageRank
	//TODO SSSP
	//�������ֵ
	//���룺�ߵ�ID
	//����graph
	static ArrayList<String> vertexList = new ArrayList<>();
	static ArrayList<String> edgeList = new ArrayList<String>();//�Լ����������еı�
	public static void main(String[] args) throws InterruptedException {
		//Step1:��graph�ж�ȡ�ߵĹ�ϵ����master���зָ�洢ÿ������ıߺ͵����Ϣ
		String fileName = "E:\\A������\\�����ݷ���\\ʵ��\\ʵ��3\\max\\graph.txt";
		readGraph(fileName);
		/*
		//�����ͱ��Ƿ��ȡ��ȷ
		for(String s :vertexList) {
			System.out.print(s+" ");
		}
		System.out.println();
		for(String s :edgeList) {
			System.out.println(s);
		}*/
		//���зָ�
		Master master = new Master();
		//master.partition(vertexList,edgeList);//���зָ�
		//�ڶ��Σ���ֱ��load
		master.load("E:\\A������\\�����ݷ���\\ʵ��\\ʵ��3\\max\\partition\\");
		
		//Step2����ÿ���飬���г�ʼ��
		//master.masterInit();

		//Step3��ִ��Master��������
		master.main();
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
				if(!vertexList.contains(split[0])) {
					vertexList.add(split[0]);
				}
				if(!vertexList.contains(split[1])) {
					vertexList.add(split[1]);
				}
				//�����б�
				edgeList.add(split[0]+" "+split[1]);
			}
			bf.close();
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
