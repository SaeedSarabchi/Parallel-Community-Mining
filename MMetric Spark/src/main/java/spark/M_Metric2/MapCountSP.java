package spark.M_Metric2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapCountSP extends Mapper<Object, Text, Text, IntWritable> 
{
	private Text myKey = new Text();
	private IntWritable one = new IntWritable(1);
	
	@Override
	public void map( Object key, Text value, Context context ) 
			throws IOException, InterruptedException 
	{
		String line = value.toString();
		String[] strings = line.split( "\t" );
		
		if ( strings.length == 2 )
		{
			myKey.set( strings[0] );
			context.write( myKey, one );
			
			myKey.set( strings[1] );
			context.write( myKey, one );
		}
	}
}