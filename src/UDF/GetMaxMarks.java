package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class GetMaxMarks extends UDF{

	public double evaluate (double math,double eng,double physics,double social)
	{
		double maxMarks=math;
		if(eng>maxMarks)
		{
			maxMarks = eng;
		}
		if(physics>maxMarks)
		{
			maxMarks=physics;
		}
		if(social>maxMarks)
		{
			maxMarks=social;
		}		
		return maxMarks;
	}
}
