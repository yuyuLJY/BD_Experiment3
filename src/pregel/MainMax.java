package pregel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class MainMax {
	//TODO PageRank
	//TODO SSSP
	//求出最大的值
	//输入：边的ID
	//读入graph
	static ArrayList<String> vertexList = new ArrayList<>();
	static ArrayList<String> edgeList = new ArrayList<String>();//自己的区域所有的边
	public static void main(String[] args) throws InterruptedException {
		//Step1:从graph中读取边的关系，在master进行分割，存储每个区域的边和点的信息
		String fileName = "E:\\A大三下\\大数据分析\\实验\\实验3\\max\\graph.txt";
		readGraph(fileName);
		/*
		//检验点和边是否读取正确
		for(String s :vertexList) {
			System.out.print(s+" ");
		}
		System.out.println();
		for(String s :edgeList) {
			System.out.println(s);
		}*/
		//进行分割
		Master master = new Master();
		//master.partition(vertexList,edgeList);//进行分割
		//第二次，就直接load
		master.load("E:\\A大三下\\大数据分析\\实验\\实验3\\max\\partition\\");
		
		//Step2：对每个块，进行初始化
		//master.masterInit();

		//Step3：执行Master超步计算
		master.main();
	}
	
	public static void readGraph(String fileName){
		//按行读取
		ArrayList<String> arrayList = new ArrayList<>();
		try {
			FileReader fr = new FileReader(fileName);
			BufferedReader bf = new BufferedReader(fr);
			String str;
			// 按行读取字符串
			while ((str = bf.readLine()) != null) {
				String[] split = str.split("\t");//分隔开
				//填充点列表
				if(!vertexList.contains(split[0])) {
					vertexList.add(split[0]);
				}
				if(!vertexList.contains(split[1])) {
					vertexList.add(split[1]);
				}
				//填充边列表
				edgeList.add(split[0]+" "+split[1]);
			}
			bf.close();
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
