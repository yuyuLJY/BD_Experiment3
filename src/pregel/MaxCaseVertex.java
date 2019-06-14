package pregel;

import java.util.Arrays;

public class MaxCaseVertex extends Vertex{

	@Override
	public double Compute(String message,String vid) {
		// TODO 执行计算最大值的Compute
		//形式："1 6"|"2 2"
		Communication c = new Communication();
		double max = Double.valueOf(c.value.get(vid));
		if(!message.equals("")) {
			String split1[] = message.split("\\|");
			for(int i=0;i<split1.length;i++){
				String split2[] = split1[i].split(" ");
				if(Double.valueOf(split2[1])>max) {
					max = Double.valueOf(split2[1]);
				}
			}
		}
		return max;
	}
}
