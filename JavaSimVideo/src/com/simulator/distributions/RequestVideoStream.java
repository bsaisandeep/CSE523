package com.simulator.distributions;

import java.util.Arrays;
import java.util.Random;
import java.util.Vector;
import java.io.*;
/*import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;*/

import com.simulator.constants.Constants;;

public class RequestVideoStream {

	private String requestStreamFile; 	//the final output file containing the requests
	private String statisticsFile;    	//some statistics about the generated stream(pop and file size of unique docs)
	private int firstId;
	private int noofDistinctDocs;  		//number of distinct documents requested
	private long totalNoofRequests; 	//the total no of requests generated
	private double videoZipfSlope; 		//Default value = 0.668
	private double weibullK; 			//Default value = 0.513
	private double weibullL;			//Default value = 6010
	private int video_pop_distr;
	private Request uniqueDoc[];  		// the unique files in the workload
	private Distributions distributions; 
	private double videoInterArrivalTime;
	private double videoRequestInterArrivalTime;
	
	public RequestVideoStream(String initrequestStreamFile, String initstatisticsFile, double videoZipfSlope_val, 
			long inittotalNoofRequests, double redundancy, double weibullK_val, double weibullL_val, int initfirst_ID, 
			Distributions distributions_val, double lastObjectEndTime, int video_pop_distr_val) {
		
		/* Initialize the parameters */
		requestStreamFile = initrequestStreamFile;
		requestStreamFile = requestStreamFile.concat(".video");
		
		statisticsFile = initstatisticsFile;
		statisticsFile = statisticsFile.concat(".video");
		
		firstId = initfirst_ID;
		totalNoofRequests 	= inittotalNoofRequests;
		noofDistinctDocs 	= Math.max((int)((1-redundancy) * totalNoofRequests),1);
		
		System.out.println("Video #objects = " + noofDistinctDocs  + 
						   ", redundancy = "  +	redundancy     + 
					       ", totalNoofRequests = "   + totalNoofRequests + "\n");
		
		videoZipfSlope = videoZipfSlope_val;
		weibullK = weibullK_val;
		weibullL = weibullL_val;
		distributions = distributions_val;
		video_pop_distr = video_pop_distr_val;
		
		videoInterArrivalTime = lastObjectEndTime/noofDistinctDocs;
		videoRequestInterArrivalTime = lastObjectEndTime/totalNoofRequests;
			
	}
	
	private int[] GeneratePopularities(int distr)
	{
	
		float popularity[];  //pointer to a list of integers  representing the pops
		int popularities[];
		
		Random rnd = new Random();
		rnd.setSeed(time(0));

		popularity = new float[noofDistinctDocs];
		popularities = new int[noofDistinctDocs];
		
		//first deal with the non 1-timers
		float popularitySum = 0; //sum of the total popularity

		switch (distr)	{
			case 1: //Gamma
				    System.out.println("Gamma distribution not supported yet...\n");
					System.exit(0);
					break;
			case 2: //Zipf
			{
					// Estimate k in the zipf formula. The middle one timer 
					// has a popularity of 1, hence it can be used to 
					// estimate k.
					float k =(float)Math.pow(noofDistinctDocs, videoZipfSlope);

					for (int rank =1; rank<=noofDistinctDocs; rank++) {
						float freq = (float)(k / Math.pow(rank, videoZipfSlope));
						if (freq < 2)
						freq = 2; //do not allow any 1-timer here
						popularity[rank-1] = freq;
						popularitySum += freq;
					}

					break;
			}
			default: {
				//Weibull
				for (int rank =1; rank<=noofDistinctDocs; rank++) {
					float freq = (float)distributions.Weibull(rank,weibullK, weibullL);
					popularity[rank-1] = freq;
					popularitySum += freq;
				}

			break;
			}
		}

		/* 
		 * If there is a difference between sum of popularities and the 
		 * desired noof requests then we need to adjust the popularities by 
		 * scaling either upward or downward
		 */
		if ((totalNoofRequests - popularitySum ) != 0) {
			float scalingFactor = (float)(totalNoofRequests)/popularitySum;

			for (int i=0; i < noofDistinctDocs; i++) {
				popularities[i] = (int)(popularity[i] * scalingFactor);
			}
		}

		/* Sort before returning the array into ascending order */
		Arrays.sort(popularities);
		return popularities;
	}
	
	public int[] GenerateFileSizes()
	{
	    int filesizes[] = new int[noofDistinctDocs];
		int normDist_1_Mean = 16;
		int normDist_2_Mean = 208;
		int normDist_3_Mean = 583;
		int normDist_4_Mean = 295;
		int normDist_1_Std = 62;
		int normDist_2_Std = 58;
		int normDist_3_Std = 16;
		int normDist_4_Std = 172;

		float dist_1_perc = (float)0.486;
		float dist_2_perc = (float)0.262;
		float dist_3_perc = (float)0.027;

		float point_1 = dist_1_perc;
		float point_2 = point_1 + dist_2_perc;
		float point_3 = point_2 + dist_3_perc;

		int mean, std;

	    //TBD...
	    //Yield a single value for the moment
	    //i.e. the median video file size 8.215MB
	  	for (int i=0; i<noofDistinctDocs; i++) {
			double length = -1;
			float randNum = (float)distributions.Uniform01();
			
			if (randNum <= point_1)	{
				mean = normDist_1_Mean;
				std  = normDist_1_Std;
			}
			else if ((point_1 < randNum) && (randNum <= point_2)) {
				mean = normDist_2_Mean;
				std  = normDist_2_Std;
			}	
			else if ( (point_2 < randNum) && (randNum <= point_3)) {
				mean = normDist_3_Mean;
				std  = normDist_3_Std;
			}	
			else {
				mean = normDist_4_Mean;
				std  = normDist_4_Std;
			}	

			while (length <= 0)
				length = distributions.Normal(mean, std);


			filesizes[i] = (int)(length*330*1024)/8; // length*Kbps -> bytes; //In Bytes
		}

	  return filesizes;
	}
	
	public void GenerateAllRequests()
	{
		Random rnd = new Random();
		rnd.setSeed(time(0));
		String br = "\r\n";
		
		try {
			FileOutputStream file = new FileOutputStream(requestStreamFile); 
			DataOutputStream out = new DataOutputStream(file); 
		
			Vector<Integer> videoArrivalOrder = new Vector<Integer>();
			
			for (int i = 0; i <	noofDistinctDocs; i++) {
				videoArrivalOrder.addElement(i);
			}
			
			for (int i = 0 ; i < noofDistinctDocs ; i++) {
				Request req = uniqueDoc[i];
	
				int itemIndex = videoArrivalOrder.elementAt(i);
				double time = itemIndex * videoInterArrivalTime;
				
				/* 
				 * Alternatively... 
				 * float time = distributions->ParetoCDF(alphaBirth)*24*3600;
				 */
				
				for (int k = 0; k < req.GetFreq() ; k++) {
					time = time + videoRequestInterArrivalTime;					
					 out.writeBytes(Double.toString(time) + "\t");
		        	 out.writeBytes(Integer.toString(req.GetFileId()) + "\t");
		        	 out.writeBytes(Integer.toString(req.GetFileSize()));
		        	 out.write(br.getBytes());
				}
			}
			out.close(); 		
		}
		catch (IOException e) {
            System.out.println("IOException:");
            e.printStackTrace();
        }
	}

	private long time(int i) {
		long time = System.currentTimeMillis();
		return time;		
	}
	
	public void GenerateUniqueDocs(Node[] popCDF, int noofItems1, Node[] sizeCDF, int noofItems2)
	{
		uniqueDoc = new Request[noofDistinctDocs];

		if (uniqueDoc == null) {
			System.out.println("Error allocating memory in function GenerateUniqueDocs_P2P \n");
			System.exit(1);
		}
		
		Random rnd = new Random();
		Random rnd1 = new Random();
		
		int rand1 = distributions.UniformInt(1000);
		int rand2 = distributions.UniformInt(1000);
		int rand3 = distributions.UniformInt(1000);		
		int seed[] 	= {rand1, rand2, rand3}; //{5, 7, 9};

		rnd.setSeed(time(0));
		rnd1.setSeed(seed[1]);

		int total = 0;
		int filesize, filepop;
		double randnum;
		int r;
		Node node = new Node();

		// First generate the popularities
		for (int count=0; count < noofDistinctDocs; count++) {
			
			r = rnd.nextInt();
			randnum = (double) 1.0 * r;
			randnum = r / (1.0 * Integer.MAX_VALUE);
			
			filepop = Math.max((int)1,node.FindValue(popCDF, noofItems1, randnum));
			total += filepop;
			Request request = new Request(filepop, 0, Constants.VIDEO); //sets the file size to 0 here
			uniqueDoc[count] = request;
		}

		// Then generate the file sizes without correlation but disallowing files
		// bigger than 10K to have popularity >=100
		int count = 0;
		long sum = 0;
		int r1;
		double randnum1;
		Node node1 = new Node();

		while (count < noofDistinctDocs) {
			
			r1 = rnd.nextInt();
			randnum1 = (double) 1.0 * r1;
			randnum1 = r1 / (1.0 * Integer.MAX_VALUE);
			
			filesize = node1.FindValue(sizeCDF, noofItems2, randnum1);

			uniqueDoc[count].SetFileSize(filesize);
			count++;
			sum += filesize;
		}

		if ((totalNoofRequests - total) != 0) {
			float scalingFactor = (float)(totalNoofRequests)/(total);
			total = 0;

			for (int i=0; i < noofDistinctDocs ; i++) {
				//int freq  = (int)(uniqueDoc[i]->GetFreq() * scalingFactor);		
				int freq = (int)Math.floor(uniqueDoc[i].GetFreq() * scalingFactor + 0.5);	
				uniqueDoc[i].SetFreq(freq);
				total += freq;
			}
		}

		// Assign the file ids now starting from 0
		for (int i=0; i< noofDistinctDocs; i++)
			uniqueDoc[i].SetFileId(firstId+i);

		// Now set the cummulative probability of each request based on its popularity
		for (int j= 0; j < noofDistinctDocs; j++) {
			uniqueDoc[j].SetProb((double)uniqueDoc[j].GetFreq()/ total);
		}

		totalNoofRequests = total; //this is the new total noof requests that will be generated
	}

	public void GenerateRequestStream()
	{
		// First generate a set of popularities for the number of distinct files in the
		// workload and store it in an array. Note that there is just a pointer to this array
		System.out.println("Generating Video workload : \n");
		System.out.println("\tGenerating starting popularities...\n");
		int[] popularities = GeneratePopularities(video_pop_distr);

		// Generate the Cumulative distribution values from the popularities. Each value
		// in the popularityCDF array has two values: a unique value and the cumm. freq.
		// All the values are stored in the popularityCDF array as shown below which just
		// one pointer to the whole array. The array has noofElement1 elements.
		System.out.println("\tGenerating CDF for popularities...\n");
		Node node = new Node();
		NoofElement nele = new NoofElement();
		Node popularityCDF[] = node.CDF(popularities, nele, noofDistinctDocs);
		int noofElement1 = nele.number;
		
		//Do the same above for file sizes.
		System.out.println("\tGenerating starting file sizes...\n");
		
		Random rnd = new Random();
		rnd.setSeed(time(0)); // Why ????????
		
		int[] filesizes = GenerateFileSizes();

		System.out.println("\tGenerating CDF for file sizes...\n");
		int noofElement2; //the total no of elements in CDF values
		Node[] filesizeCDF = node.CDF(filesizes, nele, noofDistinctDocs);
		noofElement2 = nele.number;
		

		// Now, use the CDFs to generate unique file sizes and popularities and introduce the
		// desired correlation. The generated info about the unique docs in the workload will be
		// stored in an array called uniquedocs, which is an instance variable in this class.
		System.out.println("\tGenerating popularities & file sizes for the distinct requests...\n");
		GenerateUniqueDocs(popularityCDF, noofElement1, filesizeCDF, noofElement2);

		
		//writes the info about each unique docs into a file. The info has only 2 column. The first
		//column is the popularity of each distinct file arranged in descending order. This could
		//easily be extracted and used to plot the popularity ranking graph. The second column is the
		//file size of each distinct file in the workload, but is not sorted. You may have to sort it
		//when you extract that column to plot CDF or LLCD stuffs.
		System.out.println("\tGenerating Statistics File - docs.video...\n");
		node.OutputPopAndFileSize(statisticsFile,noofDistinctDocs, uniqueDoc, true);

		//Now, call the routine that will now generate the workload from the info already computed
		//about each distinct file using the LRU stack approach. The request is stored in the file
		//specified as input to the workload generator. The file has 2 columns: the first column is
		//the fileId and the second column is the file size.
		System.out.println("\tGenerating the requests...\n");
		System.out.println("\tGenerating All Requests File - wokload.video...\n");
		GenerateAllRequests();

		//Now all requests have been generated
		System.out.println("Done with Video Traffic!\n\n");
	}
	
	public int LastObjectId() {
		return noofDistinctDocs-1;
	}
}
	
