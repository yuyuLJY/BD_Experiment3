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
	
	//master先按照边进行分割，把分割情况保留下来，假设一共2个worker，则有两个txt
	//读取分割的情况，告诉每个worker，分割的情况
	//控制超步的节奏
	static ArrayList<String> vertexAllList = new ArrayList<>();
	static ArrayList<String> edgeAllList = new ArrayList<String>();
	static ArrayList<String[]> vertexList = new ArrayList<>();//区域i的点的情况<[0,2]><[1,3]>
	static ArrayList<String[]> edgeList = new ArrayList<String[]>();//区域i的边的情况<["1 0"]["0 1"]><>
	static int workerNum = 2;//worker的个数
	int currentWorkId;
	static boolean finishmentStatus[] = new boolean[workerNum];
	//static Map<String, String> value = new HashMap<String, String>();//存点上的值
	public void partition(ArrayList<String> vList,ArrayList<String> eList){
		/**
		 * 使用  策略进行划分
		 * @param vList顶点列表
		 * @param eList边列表，其中每一条边存在一个数组里，形式：[出边，入边]
		 * @return 把结果写进磁盘(txt)
		 */
		//Step1:先对点进行划分
		//有多少条边，就有多少个String元素,每个String记录了该worker包含哪些点
		//<"3 1","0 2">
		ArrayList<String> vertexPerWorker = new ArrayList<>();
		for(int i=0;i<workerNum;i++) {//先初始化装worker的点的String
			vertexPerWorker.add("");
		}
		//TODO 验证是否添加了那么多e.g：2
		System.out.println("vertex的大小："+vertexPerWorker.size());
		
		for(int i=0;i<vList.size();i++) {//遍历点列表
			int workerId = Integer.valueOf(vList.get(i))%workerNum;
			String s = vertexPerWorker.get(workerId);//取出原来该区域有什么
			s = s+vList.get(i)+" ";//添加上新的点
			vertexPerWorker.set(workerId, s);
		}
		//TODO 验证是否记录的每个worker的点都正确
		for(int i = 0;i<vertexPerWorker.size();i++) {
			System.out.println("区域"+i+"的点 "+vertexPerWorker.get(i));
		}
		
		//Step2：按照每个worker区域有的边，添加每个区域应该对应的边
		//<"1 3|2 3|2 1|","1 0|3 2|2 1|">
		ArrayList<String> edgePerWorker = new ArrayList<>();
		for(int i=0;i<workerNum;i++) {//先初始化装worker的点的String
			edgePerWorker.add("");
		}
		
		for(int i = 0;i<vertexPerWorker.size();i++) {
			//取出区域i的边String
			String edgeWorkerI = edgePerWorker.get(i);
			
			//查看区域i有哪些点
			String split[] = vertexPerWorker.get(i).split(" ");
			for(int j=0;j<split.length;j++) {//遍历该区域的点
				//把边添加进来
				String vertex = split[j];
				//System.out.println("区域"+i+"的点："+vertex);
				for(String e :eList) {
					//该边包含区域内的点，同时，这条边还没有添加进区域的edgeString
					//System.out.println("边："+e);
					if((e.contains(vertex)) && (!edgeWorkerI.contains(e))) {
						edgeWorkerI = edgeWorkerI+e+"|";
					}
				}
			}
			edgePerWorker.set(i, edgeWorkerI);
		}
		//TODO 验证每个区域的边是否添加正确
		for(int i = 0;i<edgePerWorker.size();i++) {
			System.out.println("区域"+i+"的边 "+edgePerWorker.get(i));
		}
		
		//把这些信息写进TXT
		for(int i=0;i<workerNum;i++) {
			writePartitionToTxt(vertexPerWorker.get(i),edgePerWorker.get(i),i);
		}

	}
	
	public void writePartitionToTxt(String vertex,String edge,int i) {
		/**
		 * 把某个区域的点和边的信息写到txt
		 * @param vertex 某个worker区域的点,形式：<"3 1","0 2">
		 * @param edge 某个worker区域的边,形式：<"1 3|2 3|2 1|","1 0|3 2|2 1|">
		 */
		//写进txt
        String outurl="E:\\A大三下\\大数据分析\\实验\\实验3\\max\\partition\\"+String.valueOf(i)+".txt";
        File outfile=new File(outurl);
        try {
            //如果写入的文件不存在创建新文件
            if(!outfile.exists()){
                outfile.createNewFile();
            }
            //文件的输入流读取文件
            FileOutputStream out=new FileOutputStream(outfile);
            //写文件
            BufferedWriter write=new BufferedWriter(new OutputStreamWriter(out));
            
            String temp="";
            //写入点
            write.write(vertex+"\r\n");
            //写入边
            write.write(edge+"\r\n");
            //String splitE[] = edge.split("\\|");
            //for(String s :splitE){
                //写入文件
                //write.write(s+"\r\n");
                //System.out.println("写入"+s);
            //}
            write.close();
            out.close();
        } catch (Exception e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
		
	}
	
	public static void load(String fileFolder) {
		/**
		 * 把图划分的结果导进来
		 * 某个worker区域的点,形式：<"3 1","0 2">
		 * 某个worker区域的边,形式：<"1 3|2 3|2 1|","1 0|3 2|2 1|">
		 * @param fileFolder 传进来的是partition信息的文件夹
		 */
		//导入相应分区个文件
		for(int i=0;i<workerNum;i++) {
			try {
				FileReader fr = new FileReader(fileFolder+String.valueOf(i)+".txt");
				BufferedReader bf = new BufferedReader(fr);
				String str;
				// 按行读取字符串
				str = bf.readLine();//第一行的信息：点
				//分割点
				String v[] = str.split(" ");
				//Collections.addAll(vertexList,v);
				vertexList.add(v);
				while ((str = bf.readLine()) != null) {
					//第一行一下的信息：边
					//分割边
					String[] split = str.split("\\|");//分隔开得到很多的边
					edgeList.add(split);
				}
				bf.close();
				fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//验证load来生成的vertexList和edgeList是否正确
		for(int i=0;i<vertexList.size();i++) {
			System.out.println("区域"+i+"的点 "+Arrays.toString(vertexList.get(i)));
		}
		for(int i=0;i<edgeList.size();i++) {
			System.out.println("区域"+i+"的边 "+Arrays.toString(edgeList.get(i)));
		}
		//强行导入值
	}
	
	
	public static void masterInit() {
		//初始化communication表
		Communication c = new Communication();
		//告诉communication有哪些点
		c.getvertexList(vertexAllList);
		System.out.println("执行initeMessageList");
		c.initeMessageList();//初始化交流交流信息
		for(int k=0;k<workerNum;k++) {//遍历每个区域
			for(int i=0;i<vertexList.get(k).length;i++) {//对区域k的点进行遍历
				String currentVertex = vertexList.get(k)[i];
				for(int j=0;j<edgeList.get(k).length;j++) {//对区域k的边进行遍历
					String split[] = edgeList.get(k)[j].split(" ");
					if(split[0].equals(currentVertex)) {
						//System.out.println("区域"+k+" 顶点"+currentVertex+" 边:"+Arrays.toString(split));
						c.setMessage(split[0],split[1], c.value.get(split[0]));
					}
				}
			}
		}
	}

	//线程执行
	public void run() {
		//每个worker执行的东东
		System.out.println("线程名称："+Thread.currentThread().getName()+"worker"+currentWorkId);
		Worker w = new Worker(vertexList.get(currentWorkId),edgeList.get(currentWorkId));
		w.startWorker();//构建信息列表
		while(w.WorkerFinishment()==false) {
			System.out.println("未结束:线程"+Thread.currentThread().getName());
		}
		//线程结束了
		finishmentStatus[Integer.valueOf(Thread.currentThread().getName())]=true;
		System.out.println("!!结束:线程"+Thread.currentThread().getName());
		
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
				if(!vertexAllList.contains(split[0])) {
					vertexAllList.add(split[0]);
				}
				if(!vertexAllList.contains(split[1])) {
					vertexAllList.add(split[1]);
				}
				//填充边列表
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
		System.out.println("==============状态============");
		String m[] = c.getMessage();
		for(int i=0;i<m.length;i++) {
			System.out.println("【"+i+"】 "+m[i]);
		}
		System.out.println("验证value");
		System.out.println("验证active");
		System.out.println("验证ifRead");
		for(String s :c.vertex) {
			System.out.println("点"+s+" value "+c.value.get(s));
		}
		for(String s :c.vertex) {
			System.out.println("点"+s+" ifRead "+c.ifRead.get(s));
		}
		for(String s :c.vertex) {
			System.out.println("点"+s+" active "+c.active.get(s));
		}
		System.out.println("==========================");
	}
	
	public void setCurrentWorkId(int i) {
		this.currentWorkId = i;
		System.out.println("currentWorkId的值："+currentWorkId);
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
		//Step1:从graph中读取边的关系，在master进行分割，存储每个区域的边和点的信息
		String fileName = "E:\\A大三下\\大数据分析\\实验\\实验3\\max\\graph.txt";
		readGraph(fileName);
		load("E:\\A大三下\\大数据分析\\实验\\实验3\\max\\partition\\");
		
		//Step2:初始化communicate列表
		masterInit();//初始化
		//TODO 验证初始化的信息是否正确
		lookOutCurrentStatus();
		
		
		//Step3:把每个区域的情况告知worker,开线程
		//TODO 仅仅一轮超步，要变成多轮
		//并行启动线程
		
		Communication c = new Communication();
		int stepNum =0;
		while(stepNum<8 && (!c.isAllNotActive())) {
			//重置
			for(int i=0;i<finishmentStatus.length;i++) {
				finishmentStatus[i] = false;
			}
			System.out.println("===================步长："+stepNum+"==========================");
			stepNum++;
			for(int i=0;i<workerNum;i++) {
				Master master = new Master();
				master.setCurrentWorkId(i);
				Thread t = new Thread(master,String.valueOf(i));
				t.start();
			}
			
			//Step4:超步同步，即判断所有的进程都执行完毕
			while(isAllWorkerFinish()==false) {
				//System.out.println("还有worker未结束");
			}
			System.out.println("所有的线程都结束了");
			
			c.clearIfRead();//下一轮开始前的准备
			//Step5：检查当轮情况
			System.out.println("Worker都执行完毕");
			lookOutCurrentStatus();
		}
	}
}
