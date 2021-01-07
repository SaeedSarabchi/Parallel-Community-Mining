/**
 * 
 */



/**
 * @author sasa
 *
 */
public class test {



	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("Hello world!");
		
		
		int startNode = 1;
		String inDir = "/Users/sasa/Dropbox/1-Uni/CMPUT_690/Project/Code of LocalCommunityDetectionWithHadoop/karate_club.dat";
		//String inDir = "/Users/sasa/Dropbox/1-Uni/CMPUT_690/Project/Code of LocalCommunityDetectionWithHadoop/graph.dat";
		String outDir = "/Users/sasa/Dropbox/1-Uni/CMPUT_690/Project/Code of LocalCommunityDetectionWithHadoop/result.txt";
		
		
		//Find Community Sequentially and compute time
		SequentialL sm = new SequentialL();
		long begin = System.currentTimeMillis();
		sm.findLCommunitySeqLocally( startNode, inDir, outDir );
		long end = System.currentTimeMillis();
		System.out.println( "Sequential program time:" + String.valueOf( (end-begin)/1000f ) );
		

	}

}
