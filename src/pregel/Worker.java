package pregel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Worker {
	//从master接收到自己区域的点+边的关系
	//记下自己的区域有哪些点，这些点是否执行完毕compute
	//该区域完成任务以后，告诉master，自己做完了
	String[] vertex ;//每个区域的所有的点
	String[] edge ;//每个区域的所有的边
	static boolean isWorkerFinishment = false;
	Worker(String[] v,String[] e){
		this.vertex = v;
		this.edge = e;
	}
	
	public void startWorker() {
		isWorkerFinishment=false;
		//对区域内的点进行遍历
		MaxCaseVertex V = new MaxCaseVertex();
		//Step1:接收信息 Step2:执行特有的compute
		Communication c = new Communication();
		MaxCaseVertex vaxvasevertex = new MaxCaseVertex();
		String[] message = c.getMessage();
		System.out.println("区域的点集："+Arrays.toString(vertex));
		for(int i=0;i<vertex.length;i++) {//对该区域的点进行遍历
			String vId = vertex[i];//点的ID
			String item = message[Integer.valueOf(vId)];//某点的信息
			c.ifRead.put(vId, "Y");
			System.out.println("线程"+Thread.currentThread().getName()+" "+"读取"+vId);
			//把接收到的值传给compute进行计算
			double oneVertexValue = vaxvasevertex.Compute(item,vId);//得到应该的值
			//System.out.println("点"+vId+" 入边"+item+" compute:"+oneVertexValue);
			//判断是否改变，改变的话，修改值；不改变，变成不活跃的点
			//System.out.println("新旧值："+c.value.get(vId)+" "+oneVertexValue);
			if(c.value.get(vId)==oneVertexValue) {
				//变成不活跃的点
				System.out.println("线程"+Thread.currentThread().getName()+" "+vId+"变成不活跃的点");
				c.active.put(vId, "N");
				V.clearVertexMessage(vId);
			}else {
				c.active.put(vId, "Y");
				c.value.put(vId, oneVertexValue);
			}
		}
		System.out.println("线程"+Thread.currentThread().getName()+" 开始发送信息----------");
		//Step3:更新状态 Step4:发送信息出去
		//要等另一个线程已经把消息读除去了才能写，该区域还活跃的点
		for(int i=0;i<vertex.length;i++) {//对点进行遍历
			String currentVertex = vertex[i];
			if(c.active.get(currentVertex).equals("Y")) {
				for(int j=0;j<edge.length;j++) {
					String split[] = edge[j].split(" ");
					//清空原来的消息列表
					//是该区域的边，同时这个点信息已经被读取了
					if(split[0].equals(currentVertex)) {
						//要等待，等到对方把信息读出来了
						while(c.ifRead.get(split[1]).equals("N")) {
							System.out.println("线程"+Thread.currentThread().getName()+" "+"点"+currentVertex+" 边"+edge[j]+" 还在等待未读的点："+split[1]);
						}
						if(c.ifModify.get(split[1]).equals("N")) {//已经被修改过了
							V.clearVertexMessage(split[1]);//没有被清空过，才去清空
						}
						V.SendMessageTo(split[0],split[1], c.value.get(split[0]));
						System.out.println("线程"+Thread.currentThread().getName()+" "+"点"+currentVertex+"向"+split[1]+"发出值"+c.value.get(split[0]));
					}
				}
			}
		}
		//end 告诉master做完了
		//没有改动的需要清空：
		for(String v :vertex) {
			if(c.ifModify.get(v).equals("N")) {
				V.clearVertexMessage(v);
			}
		}
		isWorkerFinishment=true;
	}
	
	public static boolean WorkerFinishment() {
		//告诉master，自己的区域做完了
		return isWorkerFinishment;
	}
	
}
