package spark.M_Metric2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapComputeM extends Mapper<Object, Text, Text, Text> 
{
	private ArrayList<Integer> cp = new ArrayList<Integer>();
	private Text myKey = new Text();
	private Text myValue = new Text();
	
  
	@Override
	public void setup( Context context )
	{
		cp = GlobalVariables.StringToCP( context.getConfiguration().get( "CPString" ) );
	}
	
	@Override
	public void map( Object key, Text value, Context context ) 
			throws IOException, InterruptedException 
	{
		String line = value.toString();
		String[] strings = line.split( "\t" );
		
		if( strings.length == 2 ) 
		{
			int left = Integer.parseInt( strings[0] );
			int right = Integer.parseInt( strings[1] );
			
			boolean containL = cp.contains(left);
			boolean containR = cp.contains(right);
			
			// Number indicates which position is community nodes
			// We do not process if both of the nodes are community nodes or it starts with a community node
			
			// If only one edge between two nodes
			if( containL && !containR )
			{
				myKey.set( strings[1] );
				myValue.set( strings[0]+"\t1" );
				context.write( myKey, myValue );
			}
			else if( !containL && containR )
			{
				myKey.set( strings[0] );
				myValue.set( strings[1]+"\t1" );
				context.write( myKey, myValue );
			}
			else if( !containL && !containR )
			{
				myKey.set( strings[0] );
				myValue.set( strings[1]+"\t-1" );
				context.write( myKey, myValue );
				
				myKey.set( strings[1] );
				myValue.set( strings[0]+"\t-1" );
				context.write( myKey, myValue );
			}
			
			/*
			// If two edges between two nodes
			if( !containL && containR )
			{
				myKey.set( strings[0] );
				myValue.set( strings[1]+"\t1" );
				context.write( myKey, myValue );
			}
			else if( !containL && !containR )
			{
				myKey.set( strings[0] );
				myValue.set( strings[1]+"\t-1" );
				context.write( myKey, myValue );
			}*/
		}
	}
}
