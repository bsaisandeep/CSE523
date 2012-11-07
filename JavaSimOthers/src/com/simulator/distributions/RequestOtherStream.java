package com.simulator.distributions;

import java.util.Arrays;
import java.util.Random;
import java.util.Vector;
import java.io.*;
import com.simulator.constants.Constants;

public class RequestOtherStream {
	
	private String requestStreamFile; 		//the final output file containing the requests
	private String statisticsFile;    		//some statistics about the generated stream(pop and file size of unique docs)
	
	private float other_median_object_size; //In Bytes, 5 KB
	private float other_traffic_size;
		
	private int  noofDistinctDocs;  	
	private double web_other_rel;
	private double other_size;	
	private long lastObjectEndTime;
	private long nextId;
	private double otherZipfSlope;			//Default value: 0.7
	private double other_redundancy;		//Default value = 0.5
	private long totalNoofRequests;
	
	private Request uniqueDoc[];  			// the unique files in the workload
	private Distributions distributions; 
	
	private float OtherInterArrivalTime;
	private float OtherRequestInterArrivalTime;

	
	public RequestOtherStream(String initrequestStreamFile, String initstatisticsFile, long initOtherRequests, 
								double initOther_redundancy, double initotherZipfSlope, int initnextId, 
								Distributions distributions_val, long initlastOtherReqTime, double init_other_size,
								double initweb_other_rel) {
		
		/* Initialize the parameters */
		
		requestStreamFile = initrequestStreamFile;
		requestStreamFile = requestStreamFile.concat(".other");
		
		statisticsFile = initstatisticsFile;
		statisticsFile = statisticsFile.concat(".other");
		
		totalNoofRequests = initOtherRequests;
		other_redundancy = initOther_redundancy;
		noofDistinctDocs = Math.max((int)((1-other_redundancy) * totalNoofRequests),1);

		System.out.println("Other #objects = " + noofDistinctDocs  + 
				           ", redundancy = "  +	other_redundancy     + 
			               ", totalNoofRequests = "   + totalNoofRequests + "\n");
		
		otherZipfSlope = initotherZipfSlope;
		nextId = initnextId;
		distributions = distributions_val;
		lastObjectEndTime = initlastOtherReqTime;		
		other_size = init_other_size;		
		web_other_rel = initweb_other_rel;
		
		OtherInterArrivalTime = (float) lastObjectEndTime/noofDistinctDocs;
		OtherRequestInterArrivalTime = (float) lastObjectEndTime/totalNoofRequests;	
	}
	
	private long time(int i) {
		long time = System.currentTimeMillis();
		return time;		
	}
	
	private int[] GeneratePopularities() {
		
		int popularities[];  //pointer to a list of integers  representing the pops
		float popularity[];
		
		Random rnd = new Random();
		rnd.setSeed(time(0));
		
		int rand1 = distributions.UniformInt(1000);
		int rand2 = distributions.UniformInt(1000);
		int rand3 = distributions.UniformInt(1000);
		
		
		int seed[] 	= {rand1, rand2, rand3}; //{5, 7, 9};


		popularity = new float[noofDistinctDocs];
		popularities = new int[noofDistinctDocs];
		
		// Estimate k in the zipf formula. The middle one timer has a 
		// popularity of 1, hence it can be used to estimate k
		//float k = pow(noofDistinctDocs, zipfSlope);

		float floatSum=0;
		for (int rank=1; rank <= noofDistinctDocs; rank++) {
			floatSum = (float)(1 / Math.pow(rank, otherZipfSlope));
		}
		
		float k = 1/floatSum;
		
		// First deal with the non 1-timers
		int popularitySum = 0; //sum of the total popularity

		for (int rank=1; rank <= noofDistinctDocs; rank++) {
			float freq = (float)(k / Math.pow(rank, otherZipfSlope));
			if (freq < 2)
				freq = 1; 
			popularity[rank-1] = freq;
			popularitySum += freq;
			
		}

		// If there is a difference between sum of popularities and the 
		// desired noof requests then we need to adjust the popularities by
		// scaling either upward or downward
		if ((totalNoofRequests - popularitySum ) != 0) {
			float scalingFactor = (float)(totalNoofRequests)/popularitySum;

			for (int i=0; i < noofDistinctDocs; i++) {
				popularities[i] = (int)(popularity[i] * scalingFactor);
				if (popularities[i] < 2)  popularities[i] = 1; 
			}
		}

		// Sort before returning the array into ascending order
		Arrays.sort(popularities);
		//qsort(popularity, noofDistinctDocs, sizeof(int), (int(*)(const void *, const void *))compare);

		return popularities;
	}
	
	int[] GenerateFileSizes()
	{
	    int[] filesizes = new int[noofDistinctDocs];

		//TBD..Fixed file size for the time being
		for (int i =0; i<noofDistinctDocs; i++)	{
			filesizes[i] = (int)other_size; //In Bytes
		}
		return filesizes;
	}
	
	void GenerateAllRequests()
	{
	
		String br = "\r\n";
		
		try {
		
			FileOutputStream file = new FileOutputStream(requestStreamFile); 
			DataOutputStream out   = new DataOutputStream(file); 

			Vector<Integer> allOtherObjectRequests = new Vector<Integer>();;
			int	totalNumRequestsCounter = 0;
			
			for (int i = 0; i <	noofDistinctDocs; i++) {
				Request req = uniqueDoc[i];
				int freq = req.GetFreq();
				totalNumRequestsCounter = totalNumRequestsCounter+freq;
				
				if (freq == 0) { 
					System.out.println("ZERO"); 
					System.exit(1);
				}
				
				for (int l=0; l < freq; l++) {
					allOtherObjectRequests.addElement(i);
				}
			}
			
			System.out.println("totalNumRequestsCounter = " + totalNumRequestsCounter + 
					" allOtherObjectRequests size = " + allOtherObjectRequests.size() + 
					" arrival rate = "+ Constants.ARRIVAL_RATE*web_other_rel);
			
			float time = 0;
			while (totalNumRequestsCounter > 0)	{
				int reqIndex = distributions.UniformInt(totalNumRequestsCounter);
				int itemIndex = allOtherObjectRequests.elementAt(reqIndex);
				allOtherObjectRequests.removeElementAt(reqIndex);
				//allOtherObjectRequests.removeElementAt(allOtherObjectRequests.firstElement()+reqIndex);
				Request req = uniqueDoc[itemIndex];
				
				time = time + (float)distributions.Exponential(1.0/(Constants.ARRIVAL_RATE/web_other_rel));
				out.writeBytes(Float.toString(time) + "\t");
				out.writeBytes(Integer.toString(req.GetFileId()) + "\t");
	       	 	out.writeBytes(Integer.toString(req.GetFileSize()));
	       	 	out.write(br.getBytes());
				
	       	 	totalNumRequestsCounter--;
			}
				
				out.close();
			}
			
			catch (IOException e) {
	            System.out.println("IOException:");
	            e.printStackTrace();
	        }
	}
	
	void GenerateUniqueDocs(Node[] popCDF, int noofItems1, Node[] sizeCDF, int noofItems2)
	{
		uniqueDoc = new Request[noofDistinctDocs];

		if (uniqueDoc == null) {
			System.out.println("Error allocating memory in function RequestOtherStream::GenerateUniqueDocs \n");
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
		int fileSizeRounded;
		int r;
		double randnum;
		Node node = new Node();

		// First generate the popularities
		for (int count=0; count < noofDistinctDocs; count++) {
			
			r = rnd.nextInt();
			randnum = (double) 1.0 * r;
			randnum = r / (1.0 * Integer.MAX_VALUE);
			
			filepop = Math.max((int)1,node.FindValue(popCDF, noofItems1, randnum));
			total += filepop;
			Request request = new Request(filepop, 0, Constants.OTHER); //sets the file size to 0 here
			uniqueDoc[count] = request;
		}

		// Then generate the file sizes without correlation
		int count = 0;
		int sum = 0;
		int r1;
		double randnum1;
		Node node1 = new Node();


		while (count < noofDistinctDocs) {
			r1 = rnd1.nextInt();
			randnum1 = (double) 1.0 * r1;
			randnum1 = r1 / (1.0 * Integer.MAX_VALUE);
			
			filesize = node1.FindValue(sizeCDF, noofItems2, randnum1);
			uniqueDoc[count].SetFileSize(filesize);
			count++;
			sum += filesize;
		}

		// We have to sort them first in descending order of popularity
		Arrays.sort(uniqueDoc);
		//qsort(uniqueDoc, noofDistinctDocs, sizeof(Request*), (int(*)(const void *, const void *))compare2);

		if ((totalNoofRequests - total) != 0) {
			
			float scalingFactor = (float)(totalNoofRequests)/(total);
			total = 0;

			for (int i=0; i < noofDistinctDocs ; i++) {
				int freq = (int)Math.floor(uniqueDoc[i].GetFreq() * scalingFactor + 0.5);
				uniqueDoc[i].SetFreq(freq);
				total += freq;
			}
			
		}

		// Assign the file ids now starting from 0
		for (int i=0; i< noofDistinctDocs; i++)
			uniqueDoc[i].SetFileId((int)nextId+i);

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
		System.out.println("Generating Other workload:\n");
		System.out.println("\tGenerating starting popularities...\n");
		int[] popularities = GeneratePopularities();
		//int[] popularities = GeneratePopularities(video_pop_distr);

		// Generate the Cumulative distribution values from the popularities. Each value
		// in the popularityCDF array has two values: a unique value and the cumm. freq.
		// All the values are stored in the popularityCDF array as shown below which just
		// one pointer to the whole array. The array has noofElement1 elements.
		System.out.println("\tGenerating CDF for popularities...\n");
		int noofElement1; //the total no of elements in CDF values
		Node node = new Node();
		NoofElement nele = new NoofElement();
		Node popularityCDF[] = node.CDF(popularities, nele, noofDistinctDocs);
		noofElement1 = nele.number;
		
		//Do the same above for file sizes.
		System.out.println("\tGenerating starting file sizes...\n");
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

		// Wwrites the info about each unique docs into a file. The info has only 2 column. The first
		// column is the popularity of each distinct file arranged in descending order. This could
		// easily be extracted and used to plot the popularity ranking graph. The second column is the
		// file size of each distinct file in the workload, but is not sorted. You may have to sort it
		// when you extract that column to plot CDF or LLCD stuffs.
		System.out.println("\tGenerating Statistics File - docs.other...\n");
		node.OutputPopAndFileSize(statisticsFile,noofDistinctDocs, uniqueDoc, true);

		//Now, call the routine that will now generate the workload from the info already computed
		//about each distinct file using the LRU stack approach. The request is stored in the file
		//specified as input to the workload generator. The file has 2 columns: the first column is
		//the fileId and the second column is the file size.
		System.out.println("\tGenerating the requests...\n");
		GenerateAllRequests();

		//Now all requests have been generated
		System.out.println("Done with Other Traffic!\n\n");
	}
	
	public int LastObjectId(){
		return noofDistinctDocs-1;
	}

}
