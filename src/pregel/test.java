package pregel;

public class test  implements Runnable{
	//static String[] 
	public void run() {
		
		for(int i=0;i<3;i++) {
			//System.out.println(i);
			System.out.println("�߳����ƣ�"+Thread.currentThread().getName()+" ��ֵ��"+i);
		}
	}
	
	public static void main(String[] args) {
		test test = new test();
		for(int i=0;i<4;i++) {
			Thread t = new Thread(test,String.valueOf(i));
			t.start();
			//System.out.println("main"+i);
		}
		System.out.println("!!!");
	}
}
