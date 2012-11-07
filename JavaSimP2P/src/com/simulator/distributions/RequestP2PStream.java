package com.simulator.distributions;
import java.io.*;
import java.util.*;

import com.simulator.constants.Constants;

public class RequestP2PStream {

	//private section:
	private String requestStreamFile; 	//the final output file containing the requests
	//private String zipfFile;          	//this file will contain the zipf ranking and freq
	private String statisticsFile;    	//some statistics about the generated stream(pop and file size of unique docs)
	private int firstId;
	private double MZSlope;         		//zipf parameter 
	private double p2p_size_median;
	private int   noofDistinctDocs;  	//number of distinct documents requested
	private long   totalNoofRequests; 	//the total no of requests generated
	//private int   maxNoofRequestsPerObject; //the max no of requests generated for a single object

	private int MZplateau; //The plateau factor of Mandelbrot-Zipf distribution
	private double tracesTau;			//Parameters for the exponential decay function
	private double tracesLamda;
	private double tracesSeeding;
	private double interTorrentInterval;

	private	double lastObjectReqTime;
	private Request uniqueDoc[];  // the unique files in the workload
	private Distributions distributions; 
	private boolean fixedP2PSize;

	public RequestP2PStream(String initRequestStreamFile, String initStatisticsFile,
			double initMZSlope, long initTotalNoofRequests, double redundancy,
			int MZplateau_val, double tracesTau_val, double tracesLamda_val,
			int torrentInterarrival_val, double p2p_size_median_val, int first_ID,
			Distributions distrib, boolean fixedP2PSize_val)
	{
		requestStreamFile = initRequestStreamFile;
		requestStreamFile = requestStreamFile.concat(".p2p");

		statisticsFile = initStatisticsFile;
		statisticsFile = statisticsFile.concat(".p2p");

		firstId 			= first_ID;
		MZSlope	 			= initMZSlope;
		totalNoofRequests 	= initTotalNoofRequests;
		noofDistinctDocs 	= Math.max((int)((1-redundancy) * totalNoofRequests),1);
		
		System.out.println("P2P #objects = " + noofDistinctDocs  + 
						   ", redundancy = "  +	redundancy     + 
					       ", totalNoofRequests = "   + totalNoofRequests + "\n");
		
		MZplateau 			= (int)MZplateau_val;
		tracesTau 			= tracesTau_val;
		tracesLamda 		= tracesLamda_val;  //arrivals per hour
		tracesSeeding 		= 8.42; //1/gamma, hours
		p2p_size_median 	= p2p_size_median_val;
		interTorrentInterval= torrentInterarrival_val;//(3600 / 0.9454); //seconds between torrent arrival
		distributions 		= distrib;
		lastObjectReqTime 	= 0;
		fixedP2PSize		= fixedP2PSize_val;

	}

	private int[] GeneratePopularities(int q){

		int popularity[];  			//pointer to a list of integers  representing the pops
		
		Random rnd = new Random();
		rnd.setSeed(time(0));


//		int rand1 = distributions.UniformInt(1000);
//		int rand2 = distributions.UniformInt(1000);
//		int rand3 = distributions.UniformInt(1000);
//
//		int seed[] 	= {rand1, rand2, rand3}; //{5, 7, 9};


		if ((popularity = new int[noofDistinctDocs]) == null)
		{
			System.out.println("Error allocating memory in GeneratePopularities\n");
			System.exit(1);
		}

		float k = 0;
		float kSum = 0;
		for (int i = 1; i <= noofDistinctDocs; i++)
		{
			kSum += 1 / Math.pow( (i + q), MZSlope);
		}

		k = 1 / kSum;

		// First deal with the non 1-timers
		float popularitySum = 0; //sum of the total popularity
		for (int rank =1; rank<=noofDistinctDocs; rank++)
		{
			float freq = (float)(k / Math.pow(rank + q , MZSlope));

			popularity[rank-1] = (int)Math.ceil(freq);
			popularitySum += freq;
		}

		// If there is a difference between sum of popularities and  the desired noof requests
		// then we need to adjust the popularities by scaling either upward or downward
		if ((totalNoofRequests - popularitySum) != 0)
		{

			float scalingFactor = (float)(totalNoofRequests)/popularitySum;

			for (int i=0; i < noofDistinctDocs; i++)
			{
				popularity[i] = (int)(popularity[i] * scalingFactor);
			}
		}

		// Sort b4 returning the array into ascending order
		Arrays.sort(popularity);
		//qsort(intPopularity, noofDistinctDocs, sizeof(int), (int(*)(const void *, const void *))compare);
		return popularity;

	} //generates a list of popularities
	//following the zipf distribution. 

	private int[] GenerateFileSizes(){
		int filesizes[] = new int[noofDistinctDocs];
		long sum_filesizes = 0;
		double scaling_factor;
		double workload_size;
		
		for (int i=0; i < noofDistinctDocs; i++)
		{
			if (!fixedP2PSize) {
				filesizes[i] = (SampleFileSize() % 1024)*1024*1024;	//p2p_size_median; //In Bytes
			}
			else {
				filesizes[i] = (int) p2p_size_median*1024;		//p2p_size_median is in KB
			}
			sum_filesizes += filesizes[i];
		}
		
		workload_size = p2p_size_median*totalNoofRequests*1024;
		scaling_factor = workload_size/sum_filesizes;
		
		for(int i=0; i < noofDistinctDocs; i++)
		{
			filesizes[i] = (int) (filesizes[i]*scaling_factor); 
		}

		return filesizes;
	} 

	// CDF - produce a cummulative probability from the input list and returns the result as another list

	private void GenerateUniqueDocs(Node popCDF[], int noofItems1, Node sizeCDF[], int noofItems2)
	{
		uniqueDoc = new Request[noofDistinctDocs];

		if (uniqueDoc == null)
		{
			System.out.println("Error allocating memory in function GenerateUniqueDocs_P2P \n");
			System.exit(1);
		}
		
		Random rnd = new Random();
		Random rnd1 = new Random();

		long rand1 = distributions.UniformInt(1000);
		long rand2 = distributions.UniformInt(1000);
		long rand3 = distributions.UniformInt(1000);

		long seed[] 	= {rand1, rand2, rand3}; //{5, 7, 9};

		rnd.setSeed(time(0));
		rnd1.setSeed(seed[1]);

		int total = 0;
		int filesize, filepop;
		double randnum;
		int r;
		Node node1 = new Node();

		// First generate the popularities
		for(int count=0; count < noofDistinctDocs; count++)
		{
			r = rnd.nextInt();
			randnum = (double) 1.0 * r;
			randnum = r / (1.0 * Integer.MAX_VALUE);

			filepop = node1.FindValue(popCDF, noofItems1, randnum);
			total += filepop;
			Request request = new Request(filepop, 0, Constants.P2P); //sets the file size to 0 here
			uniqueDoc[count] = request;
		}

		// Then generate the file sizes without correlation but disallowing files
		// bigger than 10K to have popularity >=100
		int count = 0;
		int sum = 0;
		int r1;
		double randnum1;
		Node node = new Node();

		while (count < noofDistinctDocs)
		{
			r1 = rnd.nextInt();
			randnum1 = (double) 1.0 * r1;
			randnum1 = r1 / (1.0 * Integer.MAX_VALUE);

			filesize = node.FindValue(sizeCDF, noofItems2, randnum1);
			uniqueDoc[count].SetFileSize(filesize);
			count++;
			sum += filesize;
		}

		// We have to sort them first in descending order of popularity
		Arrays.sort(uniqueDoc);
		//qsort(uniqueDoc, noofDistinctDocs, sizeof(Request*), (int(*)(const void *, const void *))compare2);

		if ((totalNoofRequests - total) != 0)
		{
			float scalingFactor = (float)(totalNoofRequests)/(total);
			total = 0;

			for (int i=0; i < noofDistinctDocs ; i++)
			{
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
		for (int j= 0; j < noofDistinctDocs; j++)
		{
			uniqueDoc[j].SetProb((double)uniqueDoc[j].GetFreq()/ total);
		}

		totalNoofRequests = total; //this is the new total noof requests that will be generated
	}

	//unsigned int FindValue(Node *a, int n, double item);//locates where  the random number                         
	//is located in the list and interpolate if necessary

	private void GenerateAllRequests()
	{
		Random rnd = new Random();
		rnd.setSeed(time(0));
		String br = "\r\n";
		int sumFreq = 0;

		for (int i = 0 ; i < noofDistinctDocs ; i++)
		{
			Request req = uniqueDoc[i];
			sumFreq += req.GetFreq();
		}

		float avgFreq = sumFreq / noofDistinctDocs ;

		try {

	         FileOutputStream file = new FileOutputStream(requestStreamFile); 
			 DataOutputStream out   = new DataOutputStream(file); 

			// This vector will be used to draw order indexes for distinct torrents.
			// The target is to avoid creating torrents in the order of popularity
			Vector<Integer> torrentArrivalOrder = new Vector<Integer>(noofDistinctDocs);
			for (int i = 0; i <	noofDistinctDocs; i++)
			{
				torrentArrivalOrder.add(i);
			}

			for (int i = 0 ; i < noofDistinctDocs ; i++)
			{
				Request req = uniqueDoc[i];

				// tracesTau describes the average torrent.
				// Unfortunately we do not know the median value ...
				// ... so we go with the average value, but normalize the value
				// between all torrents in the workload.
				double tau 		=  (req.GetFreq()/ avgFreq ) * tracesTau;

				int indexIndex 	= distributions.UniformInt(torrentArrivalOrder.size());
				int itemIndex 	= torrentArrivalOrder.elementAt(indexIndex);
				double time 		= itemIndex*interTorrentInterval;

				// Do check this logic later
				//torrentArrivalOrder.remove(torrentArrivalOrder.firstElement() + indexIndex);
				torrentArrivalOrder.remove(indexIndex);

				double times[] = bitTorrentInterarrivalTimes(tracesLamda, tau, tracesSeeding, req.GetFreq());

				for (int k = 0; k < req.GetFreq() ; k++)
				{
					time = time + times[k];
					//System.out.println("times[" + k +"] : " + times[k]);
					
					if (time > lastObjectReqTime)
						lastObjectReqTime = (float)time;

					out.writeBytes(Double.toString(time) + "\t\t");
					out.writeBytes(Integer.toString(req.GetFileId()) + "\t\t");
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

	// this will generate the requests into a file specified in main.cc

	private long time(int i) {
		long time = System.currentTimeMillis();
		return time;		
	}

	private int SampleFileSize()
	{
		int filesize = 0;		
		int rand = distributions.UniformInt(Constants.LINES_SAMPLES + 1);

		try{
			String curDir = System.getProperty("user.dir");
			FileInputStream fs = new FileInputStream(curDir + "\\resources\\samples");						
			BufferedReader br = new BufferedReader(new InputStreamReader(fs));
			for(int i=1; i < rand; i++)
				br.readLine();
			String text = br.readLine();
			filesize = (int)Double.parseDouble(text);			
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
		return filesize;
	}

	public int LastObjectId(){
		return noofDistinctDocs-1;
	}

	public double LastObjectReqTime(){
		return lastObjectReqTime;
	}

	public double[] bitTorrentInterarrivalTimes(double lamda, double tau, double tracesSeeding, int noofRequests)
	{
		double val[] = new double[noofRequests];

		for (int i=0; i < noofRequests; i++)
		{
			val[i] =  3600/distributions.exponentialDecayArrivalRate(lamda,tau, tracesSeeding);
		}

		Arrays.sort(val);
		return val;
	}

	/*
	 * RequestP2PStream::GenerateRequestStream
	 * 
	 * This is the main routine called after creating an object of this
	 * class. This method calls all other methods ar necessary to
	 * generate the request stream. Generating a requests stream is done
	 * in several stages. 
	 */ 

	public void GenerateRequestStream(){ //This is the main method called from main.cc
		// First generate a set of popularities for the number of distinct files in the
		// workload and store it in an array. Note that there is just a pointer to this array
		// K.Katsaros, 29/10/2009: calling Mandelbrot-Zipf instead...
		System.out.println("Generating P2P workload:\n");
		System.out.println("\tGenerating starting popularities...\n");
		int popularities[] = GeneratePopularities(MZplateau);

		// Generate the Cumulative distribution values from the popularities. Each value
		// in the popularityCDF array has two values: a unique value and the cumm. freq.
		// All the values are stored in the popularityCDF array as shown below which just
		// one pointer to the whole array. The array has noofElement1 elements.
		System.out.println("\tGenerating CDF for popularities...\n");
		Node node = new Node();
		NoofElement nele = new NoofElement();
		Node popularityCDF[] = node.CDF(popularities, nele, noofDistinctDocs);
		int noofElement1 = nele.number; //the total no of elements in CDF values

		// Do the same above for file sizes.
		// K.Katsaros, 29/10/2009: sampling BitTorrent traces instead...
		// K.Katsaros, 28/04/2011: shall consider using a distribution...
		System.out.println("\tGenerating starting file sizes...\n");
		int filesizes[] = GenerateFileSizes();
		System.out.println("\tGenerating CDF for file sizes...\n");
		Node filesizeCDF[] = node.CDF(filesizes, nele, noofDistinctDocs);
		int noofElement2 = nele.number; //the total no of elements in CDF values

		// Now, use the CDFs to generate unique file sizes and popularities and introduce the
		// desired correlation. The generated info about the unique docs in the workload will be
		// stored in an array called uniquedocs, which is an instance variable in this class.
		System.out.println("\tGenerating popularities & file sizes for the distinct requests\n");
		GenerateUniqueDocs(popularityCDF, noofElement1, filesizeCDF, noofElement2);

		//writes the info about each unique docs into a file. The info has only 2 column. The first
		//column is the popularity of each distinct file arranged in descending order. This could
		//easily be extracted and used to plot the popularity ranking graph. The second column is the
		//file size of each distinct file in the workload, but is not sorted. You may have to sort it
		//when you extract that column to plot CDF or LLCD stuffs.
		node.OutputPopAndFileSize(statisticsFile, noofDistinctDocs, uniqueDoc, true);

		//Now, call the routine that will now generate the workload from the info already computed
		//about each distinct file using the LRU stack approach. The request is stored in the file
		//specified as input to the workload generator. The file has 2 columns: the first column is
		//the fileId and the second column is the file size.
		System.out.println("\tGenerating the requests...\n");
		GenerateAllRequests();

		//Now all requests have been generated
		System.out.println("Done with P2P Traffic!\n\n");
	}

	// Might have to put finalize to convert the destructor 
	/*RequestP2PStream::~RequestP2PStream()
{
   delete [] uniqueDoc;
}*/	
}

