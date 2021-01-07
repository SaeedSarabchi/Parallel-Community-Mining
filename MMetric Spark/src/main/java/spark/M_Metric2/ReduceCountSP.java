
package spark.M_Metric2;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceCountSP extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	private IntWritable myValue = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException
	{
		int sum = 0;
		
		for ( IntWritable value: values ) 
		{
			sum += value.get();
		}
		
		myValue.set( sum );
		context.write( key, myValue );
	}
}