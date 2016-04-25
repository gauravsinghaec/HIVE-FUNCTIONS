package UDAF;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class GetMeanMarks extends UDAF{
	public static class GetIntMeanEvaluator implements UDAFEvaluator
	{
	
		PartialResult part; 
		
		public void init()
		{
			part = null;
		}
		
		public boolean iterate(DoubleWritable value)
		{
			if (value == null)
			{
				return true;
			}
			if(part == null)
			{
				part = new PartialResult();
			}
			part.result = part.result + value.get();
			part.count++;
			return true;
		}
		
		public PartialResult terminatePartial()
		{
			return part;
		}
		
		public boolean merge(PartialResult otherFile)
		{
			if(otherFile == null)
			{
				return true;
			}
			if(part == null)
			{
				part= new PartialResult();
			}
			part.result = part.result + otherFile.result;
			part.count = part.count + otherFile.count;
			return true;
		}
		
		public DoubleWritable terminate()
		{
			if ( part == null)
			{
				return null;
			}
			return new DoubleWritable( (part.result)/part.count);
		}

	}
}