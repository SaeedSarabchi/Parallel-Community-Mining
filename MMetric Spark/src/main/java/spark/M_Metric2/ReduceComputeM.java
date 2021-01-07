package spark.M_Metric2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReduceComputeM extends Reducer<Text, Text, Text, Text> 
{
	private MultipleOutputs<Text, Text> mos;
	private String dirInfo;
	private double M;
	private int inDegree;
	private int outDegree;
	
	private Text myKey = new Text();
	private Text myValue = new Text();
	
	public void setup(Context context) 
	{
		mos = new MultipleOutputs<Text, Text>( context );
		dirInfo = context.getConfiguration().get( "dirInfo" );
		String[] MDegree = context.getConfiguration().get( "MDegree" ).split(" ");
		M = Double.parseDouble( MDegree[0] );
		inDegree = Integer.parseInt( MDegree[1] );
		outDegree = Integer.parseInt( MDegree[2] );
	}
	
	@Override
	public void reduce( Text key, Iterable<Text> values, Context context )
			throws IOException, InterruptedException 
	{
		int in = 0;
		int out = 0;
		
		for( Text text: values )
		{
			String[] vStrs = text.toString().split( "\t" );
			if( vStrs.length == 2 )
			{
				int cpPosition = Integer.parseInt(vStrs[1]);

				switch( cpPosition )
				{
					case 1:
						in++;
						break;
					case -1:
						out++;
						break;
					default:
						break;
				}
			}
		}
		
		if( in != 0 )
		{
			double myMGain = (double)(inDegree+in) / (double)(outDegree-in+out);
			//Only output when myMGain > M
			if( myMGain > M )
			{
				myKey.set( key );
				myValue.set( String.valueOf(myMGain) + "\t" + String.valueOf(in) + "\t" + String.valueOf(out) );
				mos.write( myKey, myValue, generateFileName(dirInfo) );
			}
		}
	}
	
	public void cleanup( Context context ) throws IOException, InterruptedException 
	{
		 mos.close();
	}
	
	public String generateFileName( String dirInfo )
	{
		String[] infos = dirInfo.split("\t");
		return infos[0] + "/" + infos[1] + "/" + "MInfo";
	}
}