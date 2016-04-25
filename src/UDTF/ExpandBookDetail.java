package UDTF;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ExpandBookDetail extends GenericUDTF{

	private Object[] fwdObj = null;
	private PrimitiveObjectInspector bookDtlOI = null;
	
	public StructObjectInspector initialize(ObjectInspector[] arg)
	{
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		
		bookDtlOI = (PrimitiveObjectInspector) arg[0];
		
		fieldNames.add("ISBN");
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.INT));
		
		fieldNames.add("TITLE");
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		fieldNames.add("AUTHOR");
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		fwdObj = new Object[3];
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}
	
	public void process(Object[] record) throws HiveException
	{
		String bookDtl = bookDtlOI.getPrimitiveJavaObject(record[0]).toString();
		
		String str[] = bookDtl.split(",");
		fwdObj[0] = Integer.parseInt(str[0]);
		fwdObj[1] = str[1];
		fwdObj[2] = str[2];
		
		this.forward(fwdObj);
		
	}
	
	public void close()
	{
		  
	}
}
