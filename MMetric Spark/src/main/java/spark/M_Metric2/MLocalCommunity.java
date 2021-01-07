package spark.M_Metric2;


import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import org.apache.hadoop.fs.FSDataInputStream;

public class MLocalCommunity 
{
	public static void findMLocalCommunity( int startPoint, String inDir, String outDir, 
			int index, GlobalVariables gv, Map<Integer,Boolean> map ) throws Exception
	{
		
		System.out.println( "\n Hello findMLocalCommunity \n" );
		
		gv.commPoints = new ArrayList<Integer>();
		gv.commPoints.add( startPoint );
		
		gv.M = 0.0;
		gv.inDegree = 0;
		// Get the out degree of the start point
		gv.outDegree = getOutDegree( outDir, startPoint );
		
		System.out.println( "\n gv.outDegree = "+Integer.toString(gv.outDegree)+" \n" );
		
		if( gv.outDegree == -1 )
			return;
		
		// Keep finding nodes until no more nodes or maxMGain is less than current M
		while( true )
		{
			// Compute Ein and Eout and MGain
			computeM( inDir, outDir, gv, index);
			// Get the node that yields the max M
			getMaxM( outDir, gv, index, map );
			
			if( gv.maxMGain < gv.M || gv.maxNode.equals("NA") )
			{
				System.out.println( "\n  getMaxM Break!!!\n");
				break;
			}
			
			// Update community infomation
			gv.M = gv.maxMGain;
			gv.inDegree += Integer.parseInt( gv.maxInDegree );
			gv.outDegree = gv.outDegree - Integer.parseInt(gv.maxInDegree) + Integer.parseInt(gv.maxOutDegree);
			gv.commPoints.add( Integer.parseInt(gv.maxNode) );
			
			System.out.println( gv.maxNode + "\t" + gv.maxMGain + "\t" + gv.maxInDegree + "\t" + gv.maxOutDegree );
			System.out.println( GlobalVariables.CPtoString(gv.commPoints) );
		}
		
		String localCommunity = GlobalVariables.CPtoString(gv.commPoints);
		System.out.println( "Into File: " + localCommunity );
		
		// Write result to index.rst
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( URI.create(outDir),conf );
		FSDataOutputStream out = fs.create( new Path(outDir+"/"+index+".rst") );
		out.writeUTF( localCommunity );
		/*
		for( int i = 0; i < gv.commPoints.size(); i++ )
		{
			out.writeInt( gv.commPoints.get(i) );
		}*/
		
        out.close();
	}
	
	// Compute Ein and Eout and MGain
	public static void computeM( String inDir, String outDir, GlobalVariables gv, int index ) throws Exception
	{
		Configuration conf2 = new Configuration();
		
		
		// Set input and output paths
		FileSystem fs = FileSystem.get( URI.create(outDir),conf2 );
		Path inputPath = new Path( inDir );  
		Path outputPath = new Path( outDir+"/"+index );  
		if( fs.exists(outputPath) )
		{
			if( !fs.delete(outputPath,true) )
			{
				System.out.println( "\n\nDelete output file:"+outDir+" failed!\n\n" );
				return;
			}
		}
		
		SparkConf conf = new SparkConf().setAppName("ComputeM");
		//conf.set( "CPString", GlobalVariables.CPtoString(gv.commPoints) );
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//broadcast the Global Variables
		final Broadcast< GlobalVariables> broadcastedGV = sc.broadcast(gv);
		
		
		
    // Load our input data.
    JavaRDD<String> input = sc.textFile(inputPath.toString()+"/graph.dat");
    // Split up into words.
    JavaRDD<String> graph = input.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterator<String> call(String x) {
          
        	//retrieving Global Variables
        	GlobalVariables gv = broadcastedGV.getValue();
        	
        	Text myKey = new Text();
            Text myValue = new Text();
            List<String> MappedResults = new ArrayList<String>();
        	String[] lines = x.split("\n");
        	for(Iterator<String> i = Arrays.asList(lines).iterator(); i.hasNext(); ) {
        	    String item = i.next();
        	    String[] strings = item.split( "\t" );
        	    int left = Integer.parseInt( strings[0] );
    			int right = Integer.parseInt( strings[1] );
    			
    			boolean containL = gv.commPoints.contains(left);
    			boolean containR = gv.commPoints.contains(right);
    			
    			
    			
    			// Number indicates which position is community nodes
    			// We do not process if both of the nodes are community nodes or it starts with a community node
    			
    			// If only one edge between two nodes
    			if( containL && !containR )
    			{
    				myKey.set( strings[1] );
    				myValue.set( strings[0]+"\t1" );
    			
    				MappedResults.add(myKey+","+ myValue);
    			}
    			else if( !containL && containR )
    			{
    				myKey.set( strings[0] );
    				myValue.set( strings[1]+"\t1" );
    				MappedResults.add(myKey+","+ myValue);
    			}
    			else if( !containL && !containR )
    			{
    				myKey.set( strings[0] );
    				myValue.set( strings[1]+"\t-1" );
    				MappedResults.add(myKey+","+ myValue);
    				
    				myKey.set( strings[1] );
    				myValue.set( strings[0]+"\t-1" );
    				MappedResults.add(myKey+","+ myValue);
    			}    
        	}
        	
        	return MappedResults.iterator();
			
        }});
    // Transform into word and count.
    JavaPairRDD<String, String> ComputeM1 = graph.mapToPair(
      new PairFunction<String, String, String>(){
        public Tuple2<String, String> call(String x){
        	String[] strings = x.split( "," );
			return new Tuple2(strings[0], strings[1]);
			
        }}).reduceByKey(new Function2<String, String, String>(){
            public String call(String x, String y){  return x+","+y; }});
    		
    JavaPairRDD<String,String> ComputeM2 = ComputeM1.mapToPair(
    		new PairFunction<Tuple2<String, String>, String, String>() {

				public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
					// TODO Auto-generated method stub

					//retrieving Global Variables
		        	GlobalVariables gv = broadcastedGV.getValue();
				
	        		
	        		int in = 0;
	        		int out = 0;
	        		
	        		Text myValue = new Text();
	        		
	        		String[] values = t._2.split(",");
	        		for( String text: values)
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
	        			double myMGain = (double)(gv.inDegree+in) / (double)(gv.outDegree-in+out);
	        			//Only output when myMGain > M
	        			if( myMGain > gv.M )
	        			{
	        				//myKey.set( key );
	        				 myValue.set( String.valueOf(myMGain) + "\t" + String.valueOf(in) + "\t" + String.valueOf(out) );
	        				//mos.write( myKey, myValue, generateFileName(dirInfo) );
	        				return new Tuple2<String, String>(t._1, myValue.toString());
	        			}
	        		}
	          
	        		
					return new Tuple2<String, String>(null, myValue.toString());
				}
    			
    		}).filter(new Function<Tuple2<String,String>, Boolean>() {
				
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					return v1._2 != null;
				}
			});
//    		.map(
//            	      new PairFunction<String, String, String>(){
//            	          public Tuple2<String, String> call(String x){
//            	          	String[] strings = x.split( "," );
//            	  			return new Tuple2(strings[0], strings[1]);
//            	  			
//            	          }}).;

            	//mos = new MultipleOutputs<Text, Text>( context );
        		//dirInfo = context.getConfiguration().get( "dirInfo" );
        		
    // Save the word count back out to a text file, causing evaluation.
    ComputeM2.saveAsTextFile( generateFileName(outDir + "\t" + String.valueOf(index)));
    sc.close();
    
		//FileInputFormat.setInputPaths( job, inputPath );
		//FileOutputFormat.setOutputPath( job, outputPath );
		
		// Set mapper, reducer, combiner and partitioner to our own
		//job.setJar("an2.jar");
		//job.setJarByClass( CommunityDetection.class );
		//job.setMapperClass( MapComputeM.class );
		//job.setReducerClass( ReduceComputeM.class );
		
		// MultipleOutput
		//MultipleOutputs.addNamedOutput( job, "OFFindNeighbor", 
				//TextOutputFormat.class, Text.class, Text.class);
		// Prevent creating zero-sized default output
		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		// Set output's key and value
		//job.setOutputKeyClass( Text.class );
		//job.setOutputValueClass( Text.class );
		
		// The number of reduce tasks
		//DistributedFileSystem  dfs = (DistributedFileSystem)FileSystem.get(conf);  
        //DatanodeInfo[] dataNodeStats = dfs.getDataNodeStats(); 
        //int numReducer = (int)( 0.95 * dataNodeStats.length * 2 );
        //System.out.println( "Reducer Number: " + numReducer );
		//job.setNumReduceTasks( numReducer );
		
		// Running and Timing
		//job.waitForCompletion(true);
	}
	
	public static String generateFileName( String dirInfo )
	{
		String[] infos = dirInfo.split("\t");
		return infos[0] + "/" + infos[1] + "/" + "MInfo";
	}
	
	// Get the node that yields the max M
	public static void getMaxM( String dir, GlobalVariables gv, int index, Map<Integer,Boolean> map ) throws Exception
	{
		double myMGain;
		
		double maxMGain = -1.0;
		String maxNode = "NA";
		String maxInDegree = "NA";
		String maxOutDegree = "NA";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( URI.create(dir),conf );
		Path path = new Path( dir + "/" + index + "/MInfo/" );
		FileStatus[] fileStatus = fs.listStatus( path ); 
		
		System.out.println( "\n  getMaxM: File Statis Length: "+fileStatus.length+ "\n");
		
        for( int i = 0; i < fileStatus.length; i++ )
        {  
        	String fileName = fileStatus[i].getPath().getName();
            //if( !fileStatus[i].isDir() && fileName.startsWith("MInfo") ) 
        	if( !fileStatus[i].isDir() )
            {  
                FSDataInputStream dis = fs.open( fileStatus[i].getPath() );
                LineReader in = new LineReader( dis,conf );  
                System.out.println( "\n  Goes1 \n");

                Text line = new Text();
                while( in.readLine(line) > 0 )
                {
                	 System.out.println( "\n  Goes2 \n");
        			String[] key_Value = line.toString().replace(")", "").replace("(", "").split(",");
        			//System.out.println( "\n  key_Value[0] "+key_Value[0]+" \n");
        			if(!key_Value[0].equals("null"))
        			{
	        			String[] values = key_Value[1].split("\t");
	        			String[] vStrs={key_Value[0],values[0], values[1], values[2]};
	        			//System.out.println( "\n  MInfo Keys: "+key_Value[0]+" , "+values[0]+" , "+values[1] +" , "+ values[2]+" \n");
	        			myMGain = Double.parseDouble( vStrs[1] );
	        			if( myMGain > maxMGain && map.get(Integer.parseInt(vStrs[0])) == false )
	        			{
	        				maxMGain = myMGain;
	        				maxNode = vStrs[0];
	        				maxInDegree = vStrs[2];
	        				maxOutDegree = vStrs[3];
	        			}
	                }
                }

                dis.close();
                in.close();
            }  
        }
		
		gv.maxMGain = maxMGain;
		gv.maxNode = maxNode;
		gv.maxInDegree = maxInDegree;
		gv.maxOutDegree = maxOutDegree;
	}
	
	// Get the out degree of the start point
	public static int getOutDegree( String dir, int startPoint ) throws Exception
	{
		int count = -1;
		String spStr = String.valueOf(startPoint);
		boolean find = false;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get( URI.create(dir),conf );
		Path path = new Path( dir + "/count" );
		FileStatus[] fileStatus = fs.listStatus( path ); 
		
		
		
        for( int i = 0; i < fileStatus.length; i++ )
        {  
            if( !fileStatus[i].isDir() ) 
            {  
                FSDataInputStream dis = fs.open( fileStatus[i].getPath() );
                LineReader in = new LineReader( dis,conf );  

                Text line = new Text();
                while( in.readLine(line) > 0 )
                {
                	
        			String[] vStrs = line.toString().replace(")", "").replace("(", "").split(",");
        			
        			//System.out.println( "\n Umad \n");
        			//System.out.println( "\n vStrs[0] = "+vStrs[0]+"\n");
        			if( vStrs[0].equals(spStr) )
        			{
        				count = Integer.parseInt(vStrs[1]);
        				find = true;
        				break;
        			}
                }

                dis.close();
                in.close();
            }  
            
            if( find )
            	break;
        } 
        return count;
	}
}
