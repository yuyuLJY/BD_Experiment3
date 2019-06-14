package pregel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Worker {
	//��master���յ��Լ�����ĵ�+�ߵĹ�ϵ
	//�����Լ�����������Щ�㣬��Щ���Ƿ�ִ�����compute
	//��������������Ժ󣬸���master���Լ�������
	String[] vertex ;//ÿ����������еĵ�
	String[] edge ;//ÿ����������еı�
	static boolean isWorkerFinishment = false;
	Worker(String[] v,String[] e){
		this.vertex = v;
		this.edge = e;
	}
	
	public void startWorker() {
		isWorkerFinishment=false;
		//�������ڵĵ���б���
		MaxCaseVertex V = new MaxCaseVertex();
		//Step1:������Ϣ Step2:ִ�����е�compute
		Communication c = new Communication();
		MaxCaseVertex vaxvasevertex = new MaxCaseVertex();
		String[] message = c.getMessage();
		System.out.println("����ĵ㼯��"+Arrays.toString(vertex));
		for(int i=0;i<vertex.length;i++) {//�Ը�����ĵ���б���
			String vId = vertex[i];//���ID
			String item = message[Integer.valueOf(vId)];//ĳ�����Ϣ
			c.ifRead.put(vId, "Y");
			System.out.println("�߳�"+Thread.currentThread().getName()+" "+"��ȡ"+vId);
			//�ѽ��յ���ֵ����compute���м���
			double oneVertexValue = vaxvasevertex.Compute(item,vId);//�õ�Ӧ�õ�ֵ
			//System.out.println("��"+vId+" ���"+item+" compute:"+oneVertexValue);
			//�ж��Ƿ�ı䣬�ı�Ļ����޸�ֵ�����ı䣬��ɲ���Ծ�ĵ�
			//System.out.println("�¾�ֵ��"+c.value.get(vId)+" "+oneVertexValue);
			if(c.value.get(vId)==oneVertexValue) {
				//��ɲ���Ծ�ĵ�
				System.out.println("�߳�"+Thread.currentThread().getName()+" "+vId+"��ɲ���Ծ�ĵ�");
				c.active.put(vId, "N");
				V.clearVertexMessage(vId);
			}else {
				c.active.put(vId, "Y");
				c.value.put(vId, oneVertexValue);
			}
		}
		System.out.println("�߳�"+Thread.currentThread().getName()+" ��ʼ������Ϣ----------");
		//Step3:����״̬ Step4:������Ϣ��ȥ
		//Ҫ����һ���߳��Ѿ�����Ϣ����ȥ�˲���д�������򻹻�Ծ�ĵ�
		for(int i=0;i<vertex.length;i++) {//�Ե���б���
			String currentVertex = vertex[i];
			if(c.active.get(currentVertex).equals("Y")) {
				for(int j=0;j<edge.length;j++) {
					String split[] = edge[j].split(" ");
					//���ԭ������Ϣ�б�
					//�Ǹ�����ıߣ�ͬʱ�������Ϣ�Ѿ�����ȡ��
					if(split[0].equals(currentVertex)) {
						//Ҫ�ȴ����ȵ��Է�����Ϣ��������
						while(c.ifRead.get(split[1]).equals("N")) {
							System.out.println("�߳�"+Thread.currentThread().getName()+" "+"��"+currentVertex+" ��"+edge[j]+" ���ڵȴ�δ���ĵ㣺"+split[1]);
						}
						if(c.ifModify.get(split[1]).equals("N")) {//�Ѿ����޸Ĺ���
							V.clearVertexMessage(split[1]);//û�б���չ�����ȥ���
						}
						V.SendMessageTo(split[0],split[1], c.value.get(split[0]));
						System.out.println("�߳�"+Thread.currentThread().getName()+" "+"��"+currentVertex+"��"+split[1]+"����ֵ"+c.value.get(split[0]));
					}
				}
			}
		}
		//end ����master������
		//û�иĶ�����Ҫ��գ�
		for(String v :vertex) {
			if(c.ifModify.get(v).equals("N")) {
				V.clearVertexMessage(v);
			}
		}
		isWorkerFinishment=true;
	}
	
	public static boolean WorkerFinishment() {
		//����master���Լ�������������
		return isWorkerFinishment;
	}
	
}
