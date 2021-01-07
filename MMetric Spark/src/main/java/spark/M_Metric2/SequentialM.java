package spark.M_Metric2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;


public class SequentialM {
	
	
	public void findMCommunitySeqLocally( int startNode, String inDir, String outDir )
	{
		Map<Integer,Boolean> visitedRecords = new HashMap<Integer,Boolean>(); 
		Map<Integer,Integer> allOutDegrees = new HashMap<Integer,Integer>(); 
		GlobalVariables gv = new GlobalVariables();
		
		// Compute all nodes' outdegrees and initialize all visited records to false
		// FileHandling
		try
		{
            
			FileInputStream fis = new FileInputStream( inDir );
			InputStreamReader isr = new InputStreamReader(fis);
			LineNumberReader lnr = new LineNumberReader(isr);
			
			String line = null;
            while ( ( line = lnr.readLine() ) != null ) 
            {
            	String[] edge = line.split("\t");
            	int node1 = Integer.parseInt(edge[0]);
            	int node2 = Integer.parseInt(edge[1]);
            	
            	visitedRecords.put( node1, false );
            	visitedRecords.put( node2, false );
            	
        		int count1 = allOutDegrees.containsKey( node1 ) ? allOutDegrees.get(node1) : 0;
        		allOutDegrees.put( node1, count1+1 );
        		
        		int count2 = allOutDegrees.containsKey( node2 ) ? allOutDegrees.get(node2) : 0;
        		allOutDegrees.put( node2, count2+1 );
            }
            
            lnr.close();
            isr.close();
            fis.close();
		}
        catch (Exception e) 
        {
        	e.printStackTrace();
	    }
		
		visitedRecords.put(startNode, true);
		
		for( int i = 0; ; i++ )
		{
			boolean over = true;
			
			// Find local community for a particular start point
			findLocalCommunity( startNode, inDir, gv, allOutDegrees, visitedRecords );
			
			// Set the status of all community nodes to visited
			for( int j = 0; j < gv.commPoints.size(); j++ )
			{
				int node = gv.commPoints.get(j);
				visitedRecords.put( node, true );
			}
			
			// Write results to a file
			// FileHandling
			try
	        {
	            File wf = new File( outDir );
	            BufferedWriter wfbw = new BufferedWriter( new FileWriter(wf, true) );
	            
	            wfbw.write( GlobalVariables.CPtoString( gv.commPoints ) );
	        	wfbw.newLine();
	            
	            wfbw.close();
	            wfbw = null;
	            wf = null;
		    }
	        catch (Exception e) 
	        {
	        	e.printStackTrace();
		    }
			
			// If there is still node that has not been visited, set it to start point
			ArrayList<Integer> al = new ArrayList<Integer>();
			for( Map.Entry<Integer,Boolean> entry: visitedRecords.entrySet() )
			{
				
				if( entry.getValue() == false )
				{
					al.add( entry.getKey() );
				}
			}
			if ( al.size() > 0 )
			{
				Random random = new Random();
			    int index = random.nextInt( al.size() );
				startNode = al.get(index);
				visitedRecords.put( startNode, true );
				over = false;
			}

			// If no more nodes to visit
			if( over == true )	
				break;
		}
	}
	
	public void findLocalCommunity( int startNode, String inDir, GlobalVariables gv, Map<Integer,Integer> allOutDegrees, Map<Integer,Boolean> visitedRecords )
	{
		gv.commPoints = new ArrayList<Integer>();
		gv.commPoints.add( startNode );
		
		gv.M = 0.0;
		gv.inDegree = 0;
		gv.outDegree = allOutDegrees.get( startNode );
		
		
		
		while( true )
		{
			Map<Integer,String> potentialNodes = new HashMap<Integer,String>(); 
			
			// Compute Ein and Eout and MGain
			computeM( inDir, gv, potentialNodes );
			// Get the node that yields the max M
			getMaxM( gv, potentialNodes, visitedRecords );
			
			if( gv.maxMGain < gv.M || gv.maxNode.equals("NA") )
				break;
			
			// Update community infomation
			gv.M = gv.maxMGain;
			gv.inDegree += Integer.parseInt( gv.maxInDegree );
			gv.outDegree = gv.outDegree - Integer.parseInt(gv.maxInDegree) + Integer.parseInt(gv.maxOutDegree);
			gv.commPoints.add( Integer.parseInt(gv.maxNode) );
			
			
		}
	}
	
	
	public void computeM( String inDir, GlobalVariables gv, Map<Integer,String> potentialNodes )
	{
		// FileHandling
		HashMap<Integer, Integer> inDegrees =  new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> outDegrees =  new HashMap<Integer, Integer>();
		
		try
		{
			FileInputStream fis = new FileInputStream( inDir );
			InputStreamReader isr = new InputStreamReader(fis);
			LineNumberReader lnr = new LineNumberReader(isr);
			
			String line = null;
            while ( ( line = lnr.readLine() ) != null ) 
            {
            	String[] edge = line.split("\t");
            	int node1 = Integer.parseInt(edge[0]);
            	int node2 = Integer.parseInt(edge[1]);
            	int count = 0;
            	
            	
            	if ( gv.commPoints.contains( node1 ) && !gv.commPoints.contains( node2 ) )
            	{
            		count = inDegrees.containsKey( node2 ) ? inDegrees.get(node2) : 0;
            		inDegrees.put( node2, count+1 );
            	}
            	else if ( !gv.commPoints.contains( node1 ) && gv.commPoints.contains( node2 ) )
            	{
            		count = inDegrees.containsKey( node1 ) ? inDegrees.get(node1) : 0;
            		inDegrees.put( node1, count+1 );
            	}
            	else if( !gv.commPoints.contains( node1 ) && !gv.commPoints.contains( node2 ) )
            	{
            		count = outDegrees.containsKey( node1 ) ? outDegrees.get(node1) : 0;
            		outDegrees.put( node1, count+1 );
            		count = outDegrees.containsKey( node2 ) ? outDegrees.get(node2) : 0;
            		outDegrees.put( node2, count+1 );
            	}
            }
            
            lnr.close();
            isr.close();
            fis.close();
		}
        catch (Exception e) 
        {
        	e.printStackTrace();
	    }
		
		
		for( Map.Entry<Integer,Integer> entry: inDegrees.entrySet() )
		{
			int inKey = entry.getKey();
			int inValue = entry.getValue();
			
			if( inValue != 0 )
			{
				int outValue = outDegrees.containsKey(inKey) ? outDegrees.get(inKey) : 0;
				double myMGain = (double)(gv.inDegree+inValue) / (double)(gv.outDegree-inValue+outValue);
				if ( myMGain > gv.M )
				{
					String MGainInfo = String.valueOf(myMGain) + "\t" + String.valueOf(inValue) + "\t" + String.valueOf(outValue);
					potentialNodes.put(inKey, MGainInfo);
				}
			}
		}
	}
	
	
	public void getMaxM( GlobalVariables gv, Map<Integer,String> potentialNodes, Map<Integer,Boolean> visitedRecords )
	{
		double myMGain;
		
		double maxMGain = -1.0;
		String maxNode = "NA";
		String maxInDegree = "NA";
		String maxOutDegree = "NA";
		
		for( Map.Entry<Integer,String> potentialNode: potentialNodes.entrySet() )
		{
			int key = potentialNode.getKey();
			String value = potentialNode.getValue();
			
			if( visitedRecords.get(key) == false )
			{
				String[] vStrs = value.toString().split("\t");
				myMGain = Double.parseDouble( vStrs[0] );
				if( myMGain > maxMGain )
				{
					maxMGain = myMGain;
					maxNode = String.valueOf(key);
					maxInDegree = vStrs[1];
					maxOutDegree = vStrs[2];
				}
			}
		}
		
		gv.maxMGain = maxMGain;
		gv.maxNode = maxNode;
		gv.maxInDegree = maxInDegree;
		gv.maxOutDegree = maxOutDegree;
	}
	
	
	
	public void findMCommunitySeqHadoop( int startNode, String inDir, String outDir, int threshold )
	{
		
		Map<Integer,Boolean> visitedRecords = new HashMap<Integer,Boolean>(); 
		Map<Integer,Integer> allOutDegrees = new HashMap<Integer,Integer>(); 
		GlobalVariables gv = new GlobalVariables();
		StringBuilder sb = new StringBuilder( "" );
		
		// Compute all nodes' outdegrees and initialize all visited records to false
		// FileHandling
		
		
		try
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get( URI.create(inDir),conf );
			Path path = new Path( inDir );
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
	        			String[] edge = line.toString().split("\t");
	                	int node1 = Integer.parseInt(edge[0]);
	                	int node2 = Integer.parseInt(edge[1]);
	                	
	                	visitedRecords.put( node1, false );
	                	visitedRecords.put( node2, false );
	                	
	            		int count1 = allOutDegrees.containsKey( node1 ) ? allOutDegrees.get(node1) : 0;
	            		allOutDegrees.put( node1, count1+1 );
	            		
	            		int count2 = allOutDegrees.containsKey( node2 ) ? allOutDegrees.get(node2) : 0;
	            		allOutDegrees.put( node2, count2+1 );
	                }

	                dis.close();
	                in.close();
	            }
	        }
		}
        catch (Exception e) 
        {
        	e.printStackTrace();
	    }
		
		visitedRecords.put(startNode, true);
		
		
		for( int i = 0; i < threshold; i++ )
		{
			boolean over = true;
			
			// Find local community for a particular start point
			findLocalCommunityHadoop( startNode, inDir, gv, allOutDegrees, visitedRecords );
			
			// Set the status of all community nodes to visited
			for( int j = 0; j < gv.commPoints.size(); j++ )
			{
				int node = gv.commPoints.get(j);
				visitedRecords.put( node, true );
				sb.append( String.valueOf(node) + " " );
			}
			
			sb.append( "\n" );
			
			// Write results to a file
			// FileHandling
			try
	        {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get( URI.create(outDir),conf );
				FSDataOutputStream out = fs.create( new Path(outDir+"/"+"SequentialResult.txt") );			
				out.writeUTF( sb.toString() );
		        out.close();
		    }
	        catch (Exception e) 
	        {
	        	e.printStackTrace();
		    }
			
			// If there is still node that has not been visited, set it to start point
			for( Map.Entry<Integer,Boolean> entry: visitedRecords.entrySet() )
			{
				if( entry.getValue() == false )
				{
					startNode = entry.getKey();
					visitedRecords.put( entry.getKey(), true );
					over = false;
					break;
				}
			}

			// If no more nodes to visit
			if( over == true )	
				break;
		}		
	}
	
	public void findLocalCommunityHadoop( int startNode, String inDir, GlobalVariables gv, Map<Integer,Integer> allOutDegrees, Map<Integer,Boolean> visitedRecords )
	{
		gv.commPoints = new ArrayList<Integer>();
		gv.commPoints.add( startNode );
		
		gv.M = 0.0;
		gv.inDegree = 0;
		gv.outDegree = allOutDegrees.get( startNode );
		
		
		
		while( true )
		{
			Map<Integer,String> potentialNodes = new HashMap<Integer,String>(); 
			
			// Compute Ein and Eout and MGain
			computeMHadoop( inDir, gv, potentialNodes );
			// Get the node that yields the max M
			getMaxM( gv, potentialNodes, visitedRecords );
			
			if( gv.maxMGain < gv.M || gv.maxNode.equals("NA") )
				break;
			
			// Update community infomation
			gv.M = gv.maxMGain;
			gv.inDegree += Integer.parseInt( gv.maxInDegree );
			gv.outDegree = gv.outDegree - Integer.parseInt(gv.maxInDegree) + Integer.parseInt(gv.maxOutDegree);
			gv.commPoints.add( Integer.parseInt(gv.maxNode) );
			
		}
	}
	
	public void computeMHadoop( String inDir, GlobalVariables gv, Map<Integer,String> potentialNodes )
	{
		// FileHandling
		HashMap<Integer, Integer> inDegrees =  new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> outDegrees =  new HashMap<Integer, Integer>();
		
		try
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get( URI.create(inDir),conf );
			Path path = new Path( inDir );
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
	        			String[] edge = line.toString().split("\t");
	                	int node1 = Integer.parseInt(edge[0]);
	                	int node2 = Integer.parseInt(edge[1]);
	                	int count = 0;
	                	
	                	
	                	if ( gv.commPoints.contains( node1 ) && !gv.commPoints.contains( node2 ) )
	                	{
	                		count = inDegrees.containsKey( node2 ) ? inDegrees.get(node2) : 0;
	                		inDegrees.put( node2, count+1 );
	                	}
	                	else if ( !gv.commPoints.contains( node1 ) && gv.commPoints.contains( node2 ) )
	                	{
	                		count = inDegrees.containsKey( node1 ) ? inDegrees.get(node1) : 0;
	                		inDegrees.put( node1, count+1 );
	                	}
	                	else if( !gv.commPoints.contains( node1 ) && !gv.commPoints.contains( node2 ) )
	                	{
	                		count = outDegrees.containsKey( node1 ) ? outDegrees.get(node1) : 0;
	                		outDegrees.put( node1, count+1 );
	                		count = outDegrees.containsKey( node2 ) ? outDegrees.get(node2) : 0;
	                		outDegrees.put( node2, count+1 );
	                	}
	                }

	                dis.close();
	                in.close();
	            }
	        }
		}
        catch (Exception e) 
        {
        	e.printStackTrace();
	    }
		
		
		for( Map.Entry<Integer,Integer> entry: inDegrees.entrySet() )
		{
			int inKey = entry.getKey();
			int inValue = entry.getValue();
			
			if( inValue != 0 )
			{
				int outValue = outDegrees.containsKey(inKey) ? outDegrees.get(inKey) : 0;
				double myMGain = (double)(gv.inDegree+inValue) / (double)(gv.outDegree-inValue+outValue);
				if ( myMGain > gv.M )
				{
					String MGainInfo = String.valueOf(myMGain) + "\t" + String.valueOf(inValue) + "\t" + String.valueOf(outValue);
					potentialNodes.put(inKey, MGainInfo);
				}
			}
		}
	}
}

