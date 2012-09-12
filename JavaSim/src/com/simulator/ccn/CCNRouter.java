package com.simulator.ccn;

import arjuna.JavaSim.Simulation.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.BitSet;

import com.simulator.controller.SimulationController;

import com.simulator.distributions.Arrivals;
import com.simulator.distributions.PacketDistributions;
import com.simulator.enums.PacketTypes;
import com.simulator.enums.SimulationTypes;
import com.simulator.enums.SupressionTypes;
import com.simulator.packets.*;
import com.simulator.topology.Grid;

import arjuna.JavaSim.Simulation.SimulationException;

/* This class is a SimulationProcess, hence, it will get scheduled in JavaSim Scheduler. The main function is that when it is 
 * scheduled to be picked, then the CCNRouter object selected will process a packet from its queue. 
 * */
public class CCNRouter extends SimulationProcess {
	
	//static final Logger log = Logger.getLogger(CCNRouter.class);

	public static int countLocalHits = 0;
	
	public static int hops0= 0;
	public static int hops1= 0;
	public static int hops2= 0;
	public static int hops3= 0;
	public static int hops4= 0;
	public static int hops5= 0;
	public static int hops6= 0;
	public static int hops7= 0;
	public static int hops8Plus = 0;
	
	private boolean working;
	private Packets currentPacket;
	private CCNQueue packetsQ;
	private static double procDelay;
	
	/**
	 * Pending Interest Table is a map of Interest Id and as list of Integers representing routerId's of interested nodes.
	 */
	Map<IDEntry, List<PITEntry>> pit = null;
	/**
	 * forwarding table is a map of <DataPacketId,NodeId>
	 */
	Map<Integer,FIBEntry> forwardingTable = null;
	/**
	 * Routers Unique Id
	 */
	private int routerId = 0;
	/**
	 * Routers localCache. Its own Cache
	 */
	private CCNCache localStorage = null;
	/**
	 * Cache to store the data objects it received from other routers.
	 */
	private CCNCache globalCache = null;
	
	/**
	 * Counter to distinguish various log entries of this router.
	 */
	private int logCounter = 0;	
	
	/* 
	 * The value (-ve value) given to an interest packet upon creation, which is used as its interface value in the PIT entry. 
	*/	
	int defaultInterface;
	
	private BitSet interestServedBitArray[] = null;
	
	public CCNRouter (int id) {
		
		defaultInterface = 0;
		setRouterId(id);
		packetsQ = new CCNQueue(id);
		
		/* The following code initializes the PIT with values from ccn.properties file. Note that smaller sized PIT
		 * for large simulations slows down the process significantly
		 *  */
		pit = new HashMap<IDEntry,List<PITEntry>>(SimulationController.getPITSize(), (float)0.9);
		
		/* This array is used to suppress redundant/duplicate interest packets */
		interestServedBitArray = new BitSet [(int)SimulationController.getMaxSimulatedPackets()*3];
		
		/* Do not initialize the FIB, if we are running a simulation which does not have a FIB*/
		if (SimulationTypes.SIMULATION_FIB == SimulationController.getFibAvailability()) {
			forwardingTable = new HashMap<Integer, FIBEntry>(SimulationController.getFIBSize(), (float)0.9);
		}
		
		/* The localCache will always be of unlimited size */
		localStorage = new CCNCache(id, SimulationController.getLocalCacheSize(), true, 0);			
		
		
		/* Do not initialize the caches if we are running a simulation with no cache */
		if (SimulationTypes.SIMULATION_CACHE == SimulationController.getCacheAvailability()) {
			
			/* For the smaller topology (39), the following code will execute which will assign different cache sizes for the edge
			 * and core nodes */
			if (Grid.getGridSize() < 50) {
				if (id >= 8 ) {
					globalCache = new CCNCache(id, SimulationController.getEdgeCacheSize(), SimulationController.cacheType(), 1);
				}
				else {
					globalCache = new CCNCache(id, SimulationController.getCoreCacheSize(), SimulationController.cacheType(), 1);
				}
			}			
			/* For the larger topology (100), the following code will execute which will assign different cache sizes for the edge
			 * and core nodes */
			else {
				if ((id == 11) || (id == 30) || (id == 32) || (id == 44) || (id == 45) || (id == 49) || (id == 72) || (id == 70)
						|| (id == 61) || (id == 59) || (id == 51) || (id == 75) || (id == 78) || (id == 79) || (id == 80) || (id == 82) 
						|| (id == 84) || (id == 87) || (id == 88) || (id == 89) || (id == 90) || (id == 91) || (id == 92) || (id == 93)
						|| (id == 94) || (id == 95) || (id == 96) || (id == 97) || (id == 98) || (id == 99)) {
					
					globalCache = new CCNCache(id, SimulationController.getEdgeCacheSize(), SimulationController.cacheType(), 1);
				}
				else {
					globalCache = new CCNCache(id, SimulationController.getCoreCacheSize(), SimulationController.cacheType(), 1);					
				}				
			}			
		}
	}	
	
	/* The CCNRouter is a SimulationProcess, and is scheduled by JavaSim Scheduler. The following method is called when 
	 * the JavaSim scheduler selects a CCNRouter object from its queue. The CCNRouter removes a packet from its queue, 
	 * and processes it accordingly. 
	 * */
	public void run () {
		
		for (;;) {
			
			working = true;

			while (!packetsQ.isEmpty())	{
				
				CurrentTime();
				
				currentPacket = (Packets) packetsQ.remove();
							
				/* Records the time at which the packet was removed from the queue */
				//Packets.dumpStatistics(currentPacket, "DEQUEUE");
				
				/* 
				 * "Hold(getProcDelay())" represents the processing delay for a packet before the result of that
				 *  processing is reflected in the methods "interestpackethandler()" and "datapackethandler()"
				 */
				
				try	{
					Hold(getProcDelay());
				} 
				catch (SimulationException e) {}
				catch (RestartException e) {}
				
				/* Records the time at which the packet has been processed */
				//Packets.dumpStatistics(currentPacket, "PROCSED");	
				
				/* The packet will be processed according to its packet type */
				if(currentPacket.getPacketType() == PacketTypes.PACKET_TYPE_INTEREST)
					interestPacketsHandler(currentPacket);
				else if (currentPacket.getPacketType() == PacketTypes.PACKET_TYPE_DATA)
					dataPacketsHandler(currentPacket);
				
				CurrentTime();
			}

			working = false;

			/* In case the CCNRouter objects queue is empty, then we suspend the CCNRouter using "Cancel()". When it will have 
			 * packets in its queue, it will be activated and placed in the scheduler again. 
			 * */
			try	{
				Cancel();
			}
			catch (RestartException e) {}
		}
	}
	/**
	 * This is the handler  data packets in a router. It first checks if there is an entry in Pit table
	 * if it doesn't find any it just ignores the data packet. else it runs through the the linked list
	 * returned by Pit table and sends the data packet to those routers. If the router id is my own I just ignore it.
	 * After that it deletes the pit entry.
	 * @param curPackets the data packet
	 */
	
	/* The following function process data packets. When the router (CCNRouter process) is activated, it processes
	 * the first packet in the queue. If it is a data packet, then this function is called.
	 * 
	 * Flow of the function:
	 * 
	 * 1. 	Check the PIT (Index) to see if we are expecting this packet to come.
	 * 2A. 	If we have a PIT index, then firstly, we remove stalled entries from 
	 * 		the PIT table (always perform this operation before accessing PIT).
	 * 2B 	If we have no PIT index for the corresponding object, then we suppress 
	 * 		the incoming data packet function labeling it "SUPPRESSION_NO_PIT".
	 * 3. 	Continue flow from 2A.
	 * 4. 	Iterate over all PIT entries, and send packets over the interfaces in the entries.
	 * 5. 	Remove PIT index and corresponding PIT entries from PIT.
	 * 6A. 	Add object, interface number, and hops on which packet arrived into FIB, if there is no previous 
	 * 		entry for the object.
	 * 6B. 	If there is an entry, only update it if the packet received has a smaller number hops.
	 *  */
	
	public void dataPacketsHandler(Packets curPacket) {
		
		
		IDEntry dataObject = new IDEntry (curPacket.getPacketId(), curPacket.getSegmentId());		
				
		/* I got this data packet so setting its locality to false */
		curPacket.setLocality(false);
			
		IDEntry pitIndex = new IDEntry (dataObject); 
		
		/* Remove stall entries from PIT table before adding a PIT Entry */			
		List<PITEntry> pitEntries = pit.get(pitIndex);	
				
		if (pit.containsKey(pitIndex)) {					
			if (pitEntries != null) {
				
				/* Retrieve the PIT entries corresponding to the data packet (packet id & segment id)*/
				Iterator<PITEntry> stalledPITEntries = pitEntries.iterator();
				boolean removePITIndex = false;
				
				ArrayList <Packets> tempCollectionExpiredPackets = new ArrayList <Packets> (10);				
				
				/* Iterate over the PIT entries, and check for timeout condition. If anyone of the entry (interface)
				 * has timed out, then all pit entries along with the pit index are remove from the PIT */
				while(stalledPITEntries.hasNext()) {
					
					PITEntry current = stalledPITEntries.next();
					
					double pitTO = SimulationController.getPitTimeOut();
					
					if ((SimulationProcess.CurrentTime() - current.getCreatedAtTime()) >= pitTO) {
						removePITIndex = true;
					}
					
					Packets clonePac = (Packets) curPacket.clone();
					
					clonePac.setPacketType(PacketTypes.PACKET_TYPE_INTEREST);
					clonePac.setPacketId(current.getRefPacketId());
					clonePac.setRefPacketId(dataObject.getID());
					clonePac.setSegmentId(dataObject.getSegmentID());
					clonePac.setPrevHop(-1);
					clonePac.setOriginNode(-1);
					clonePac.setNoOfHops(-1);
					clonePac.setCauseOfSupr(SupressionTypes.PIT_EXPIRATION);
					clonePac.setLocality(false);
					clonePac.setExpirationCount(-1);
					clonePac.setPrimaryInterestId(current.getPrimaryInterestID());
					clonePac.setParentInterestId(-1);					
					
					tempCollectionExpiredPackets.add(clonePac);							
				}		
				
				/* If any one of the entries expires, the following code will suppress the cloned packets with "PIT_Expiration"
				 * labels. The PIT index and the corresponding PIT entries will be removed from the PIT*/
				if (removePITIndex) {
					for (int i = 0; i < tempCollectionExpiredPackets.size(); i++)
						tempCollectionExpiredPackets.get(i).finished(SupressionTypes.PIT_EXPIRATION);
					
					pit.remove(pitIndex);
					curPacket.finished(SupressionTypes.SUPRESSION_NO_PIT);
									
					return;
				}						
			}		
		}		
		else 
		/* I havn't seen this packet so discard it */
		{			
			curPacket.finished(SupressionTypes.SUPRESSION_NO_PIT);
			return;	
		}
		
		/* The following code is used to flood data packets over all the interfaces in PIT entry for this object */
		Iterator<PITEntry> itr = pitEntries.iterator();		
		
		while(itr.hasNext()) {
			PITEntry rid = itr.next();
			
			/* We shouldn't flood the data packet to same node where it came from */
			if(rid.getoutgoingInterface() != curPacket.getPrevHop()/*I changed this from getRouterID()*/) {
				
				Packets clonePac = (Packets) curPacket.clone();
				
				/* In the following 'if' statement, we "create" data packets for all those PIT entries which are also satisfied other than the one
				 * associated with the current data packets' interest id */
				if (clonePac.getRefPacketId() != rid.getRefPacketId()) {

					clonePac.setRefPacketId(rid.getRefPacketId());
					clonePac.setOriginNode(getRouterId());
					clonePac.setNoOfHops(0);
					clonePac.setCurNode(-1);
					clonePac.setPrevHop(-1);
					clonePac.setCauseOfSupr(SupressionTypes.SUPRESSION_NOT_APPLICABLE);		
					
					clonePac.setExpirationCount(rid.getNumOfTimesExpired());
					clonePac.setPrimaryInterestId(rid.getPrimaryInterestID());
					clonePac.setParentInterestId(rid.getRefPacketId());
					
					clonePac.setDataPacketId(Packets.getCurrentDataPacketId());
					Packets.incCurrentDataPacketId();
					
					Packets.dumpStatistics(clonePac, "CRTDPRDA", curPacket);	
				}
				
				sendPacket(clonePac, rid.getoutgoingInterface());				
			}
		}		
		
		/* Now remove the entry */ 
		pit.remove(pitIndex);			
		
		/* The following swapping before entering into the Global cache is because of a more logical entry relevant to "(PCKSTATUS:CRTDPRD)" 
		 * in the trace file 
		 * */
		int tempCurr = curPacket.getCurNode(); 
		int tempPreHop = curPacket.getPrevHop();
		
		curPacket.setCurNode(-1); 
		curPacket.setPrevHop(-1);
		
		/* add the data packet into my global cache */
		//log.info("Adding to global cache");	
		
		/* Cache is a Map <Integer, Packet> object, hence, we do not have to check for duplicate values */
		if (SimulationTypes.SIMULATION_CACHE == SimulationController.getCacheAvailability())
			getGlobalCache().addToCache((Packets)curPacket.clone());	
		
		curPacket.setCurNode(tempCurr); 
		curPacket.setPrevHop(tempPreHop);
		
		/* adding entry to forwarding table */
		/* 1. We check whether the FIBEntry is null or not
		 * 2. If the FIB entry is not null, then we check whether the current FIB entry is already the shortest path, in which case, we will 
		 * not update the FIB entry
		 * 3. However, if FIB entry is null, then we will add the new FIB entry
		 * */
		if (SimulationTypes.SIMULATION_FIB == SimulationController.getFibAvailability()) {
			if (forwardingTable.get(curPacket.getPacketId()) != null) {
				
				try {

					/* The following code will print to the file whenever a FIB entry is changed because of hop count */
					if (getForwardingTableEntry(curPacket.getPacketId()).getHops() > curPacket.getNoOfHops()) {
						
						/* The following four lines of code are part of the core code performing the entry into the FIB in case there is a change of 
						 * hop count 
						 * */
						FIBEntry temp = getForwardingTableEntry(curPacket.getPacketId());
						temp.setDestinationNode(curPacket.getPrevHop());
						temp.setHops(curPacket.getNoOfHops());
						
						getForwardingTable().put(curPacket.getPacketId(), temp);											
					}
				}
				catch (Exception e) {}
			}
			
			/* If there is no FIB entry for the object, then we create a new FIB entry for it */
			else 
				getForwardingTable().put(curPacket.getPacketId(), new FIBEntry (curPacket.getPrevHop(), curPacket.getNoOfHops()));	
		}
	}
	/**
	 * Interest Packet handler for the machine. It first searches in Machine caches. If it fails then adds it to PIT. 
	 * Then looks in Forwarding Table
	 * if successful forwards the Interest packet to that node. If it fails then floods all the neighboring nodes. 
	 * @param curPacket
	 */
	/* The following function process the interest packets. When the router (CCNRouter process) is activated, it processes
	 * the first packet in the queue. If it is a interest packet, then this function is called.
	 * 
	 * Flow of the function:
	 * 
	 * 1. 	Suppresses duplicate Interest packets.
	 * 2. 	Check Content Store (Global Cache & Local Storage) for object.
	 * 3A 	If object found: Reply with a data packet containing the object.
	 * 3B.	If object not found: process the packet further.
	 * 4. 	Remove stalled entries from the PIT table (always perform this operation before accessing PIT).
	 * 5. 	Check the PIT for a match - PIT index that satisfies the object and segment being requested.
	 * 6A.	PIT index exists, then add outgoing interface into PIT entries. Suppress interest packet. 
	 * 		NOTE: Do not enter duplicate interface numbers.
	 * 6B.	PIT does not exist, then create a PIT index and add outgoing interface into the corresponding PIT entry.
	 * 7.	Lookup FIB for a matching entry corresponding to the object.
	 * 8A.	FIB match, then send packet over the interface without flooding.	
	 * 8B.	FIB not found, then flood packet over all interfaces.
	 *  */
	public void interestPacketsHandler(Packets curPacket) {
		
		/* The following code is to suppress the interest packets that have already been served. 
		 * 
		 * Array of Bitsets: when their is no segmentation, the length of the bitset is 1; while
		 * when there is segmentation, the size of the bitset is dependent on the maximum number
		 * of segments possible for each Interest Id. This implementation can handle variable number
		 * of segments per object  
		 * 
		 * */		
		
		if(interestServedBitArray[curPacket.getPacketId()] != null) {
			if (interestServedBitArray[curPacket.getPacketId()].get(curPacket.getSegmentId())) {
				curPacket.finished(SupressionTypes.SUPRESSION_ALREADY_SERVED);
				//log.info("Already served interest packet "+ curPacket.getPacketId() + " with segment ID " + curPacket.getSegmentId());
				return;
			}
		}
		else {
			int sizeOfBitSet = (int) Math.ceil((double)PacketDistributions.size[curPacket.getRefPacketId()]/(double)Arrivals.getSegmentSize());
			interestServedBitArray[curPacket.getPacketId()] = new BitSet(sizeOfBitSet);
		}
		
		interestServedBitArray[curPacket.getPacketId()].set(curPacket.getSegmentId());
		
		/* The following code is called when the interest is satisfied from: (a) the Local Storage, which means, the object originally 
		 * resides on this CCNRouter; or (b) the Global Cache also known as the content store of CCN */
		
		IDEntry dataObject = new IDEntry (curPacket.getRefPacketId(), curPacket.getSegmentId());		
		Packets data_packet = getDataPacketfromCache(dataObject);
		
		/* If object is located, reply with a data packet containing the object */
		if(data_packet != null) {
			
			/* This just a reality-check code. This value is printed towards the end of the simulation */
			if (curPacket.getNoOfHops() == 0) {
				countLocalHits++;				
			}
			
			Packets clonePac = (Packets) data_packet.clone();
			clonePac.setRefPacketId(curPacket.getSourcePacketId());
			clonePac.setSegmentId(curPacket.getSegmentId());
			clonePac.setNoOfHops(0);
			
			/* The unique interest ID that has requested the object is associated with this data packet*/
			clonePac.setPrimaryInterestId(curPacket.getPrimaryInterestId());
			
			/* The number of times the interest packet had expired out before this data packet is generated */
			clonePac.setExpirationCount(curPacket.getExpirationCount());
			clonePac.setParentInterestId(curPacket.getPacketId());
			
			/* This creates a unique Id for each data packet sent */
			clonePac.setDataPacketId(Packets.getCurrentDataPacketId());
			Packets.incCurrentDataPacketId();	
			
			/* Terminate the current packet. Move this to the start of this if-statement */
			curPacket.finished(SupressionTypes.SUPRESSION_SENT_DATA_PACKET);
			
			/* Create a data packet in reply of the interest packet, and place it in the trace file */
			Packets.dumpStatistics(clonePac, "CRTDPRD");			
			
			sendPacket(clonePac, curPacket.getPrevHop());
			
			data_packet.setRefPacketId(-1);
			data_packet.setSegmentId(0);
			

			return;
		}
		
		/* Remove stall entries from PIT table before adding a PIT Entry */		
		
		IDEntry pitIndex = new IDEntry (dataObject); 		
			
		if (pit.containsKey(pitIndex)) {
			
			/* Retrieve the PIT entries corresponding to the data packet (packet id & segment id)*/
			List<PITEntry> pitEntries = pit.get(pitIndex);						
			if (pitEntries != null) {
				
				/* Iterate over the PIT entries, and check for timeout condition. If anyone of the entry (interface)
				 * has timed out, then all pit entries along with the pit index are remove from the PIT */
				
				boolean removePITIndex = false;
				
				Iterator<PITEntry> stalledPITEntries = pitEntries.iterator();				
				ArrayList <Packets> tempCollectionExpiredPackets = new ArrayList <Packets> (10);				
				
				while(stalledPITEntries.hasNext()) {
					
					PITEntry current = stalledPITEntries.next();					
					if ((SimulationProcess.CurrentTime() - current.getCreatedAtTime()) >= SimulationController.getPitTimeOut()) {
						removePITIndex = true;
					}
					
					Packets clonePac = (Packets) curPacket.clone();
					
					clonePac.setPacketId(current.getRefPacketId());
					clonePac.setRefPacketId(dataObject.getID());
					clonePac.setSegmentId(dataObject.getSegmentID());
					clonePac.setPrevHop(-1);
					clonePac.setOriginNode(-1);
					clonePac.setNoOfHops(-1);
					clonePac.setCauseOfSupr(SupressionTypes.PIT_EXPIRATION);
					clonePac.setLocality(false);
					
					clonePac.setExpirationCount(-1);
					clonePac.setPrimaryInterestId(current.getPrimaryInterestID());
					clonePac.setParentInterestId(-1);
					
					tempCollectionExpiredPackets.add(clonePac);								
				}		
				
				/* If any one of the entries expires, the following code will suppress the cloned packets with "PIT Expiration"
				 * labels. The PIT index and the corresponding PIT entries will be removed from the PIT*/
				if (removePITIndex) {
					for (int i = 0; i < tempCollectionExpiredPackets.size(); i++)
						tempCollectionExpiredPackets.get(i).finished(SupressionTypes.PIT_EXPIRATION);
					
					pit.remove(pitIndex);
				}						
			}		
		}
		
		List<PITEntry> pitEntries = pit.get(pitIndex);	
		
		/* I have seen this index, and need to append the outgoing interface in the PITEntry list. Further, I need to suppress this packet */
		if (pit.containsKey(pitIndex)) {
			
			/* If I already have this outgoing interface, then I do not add this to the PITEntry list*/
			if(!pitEntries.contains(new PITEntry(curPacket.getPrevHop())))/*!containsPITEntry(pitEntries,curPacket.getPrevHop())*/ {
					
				pitEntries.add(new PITEntry (curPacket.getPrevHop(), curPacket.getPacketId(), curPacket.getPrimaryInterestId(), SimulationController.CurrentTime(), curPacket.getExpirationCount()));
			}				

			/* Suppress the packet */
			curPacket.finished(SupressionTypes.SUPRESSION_FLOODING_PIT_HIT);			
			return;			
		}
		/* I have not seen this PIT index. Create PIT index and corresponding PIT entry in the PIT. Add the outgoing interface in the PITEntry list. Send the packet forward */
		else {
			
			List<PITEntry> newPITEntry = new ArrayList<PITEntry>();
			newPITEntry.add(new PITEntry (curPacket.getPrevHop(), curPacket.getPacketId(), curPacket.getPrimaryInterestId(), SimulationProcess.CurrentTime(), curPacket.getExpirationCount()));
			
			pit.put(pitIndex, newPITEntry);	
		}		
		
		/* If we have a FIB match, then we will not flood the packet. We will simply help it along the FIB entry. If not then
		 * we will flood the packet 
		 * */
		if (SimulationTypes.SIMULATION_FIB == SimulationController.getFibAvailability()) {
			
			FIBEntry rid = getForwardingTableEntry(curPacket.getRefPacketId());
			
			if(rid != null) {
				/* TODO
				 * I do not think we need to clone this packet!? However, I am leaving it for now as everything is working.
				 * I will address it in the next revision */ 
				sendPacket((Packets)curPacket.clone(), rid.getDestinationNode());
				curPacket.finished(SupressionTypes.SUPPRESS_FLOODING_FIB_HIT);			
			}
			/* IF we do not have FIB match, then we flood the interest packet*/
			else {
				floodInterestPacket(curPacket);			
			}	
		}
		/* If we do not have a FIB, then we flood the interest packet every time */
		else {
			floodInterestPacket(curPacket);			
		}	
	}
	
	/* Prints the current PIT entries corresponding to a particular PIT index */
	public void printPITEntry (List<PITEntry> checkPITEntry, Packets curPacket, String status) {
		
		try {
			
			Writer fs1 = new BufferedWriter(new FileWriter("dump/PITEntry.txt",true));
			fs1.write("\nStatus : " + status+ "\n");
			fs1.write("Current Node : " + getRouterId() + "\n");
			fs1.write("ObjectID : " + curPacket.getRefPacketId() + "\n");	
		
			if (checkPITEntry != null) {
				Iterator<PITEntry> itr = checkPITEntry.iterator();	
				int count = 0;
				
				while(itr.hasNext()) {
					
					count ++;
					
					PITEntry rid = itr.next();
					
						fs1.write("\nEntry : " + count + "\n");
						fs1.write("Time: " + SimulationProcess.CurrentTime() + "\n");
						fs1.write("Current Node : " + getRouterId() + "\n");
						fs1.write("PacketID : " + curPacket.getPacketId() + "\n");
						fs1.write("ObjectID : " + curPacket.getRefPacketId() + "\n");				
						fs1.write("Outgoing Interface is: " + rid.getoutgoingInterface() + "\n");
						fs1.write("InterestID: " + rid.getRefPacketId() + "\n");
						fs1.write("Timestamp: " + rid.getCreatedAtTime() + "\n" + "\n" + "\n" + "\n");														
					}	
				}
		
			fs1.close();
		}
		catch (IOException e){}	
	}

	/**
	 * gets the forwardingTableEntry for this packetid.
	 * @param packetId
	 * @return
	 */
	/* Searches FIBs (InterestPacketHander() comment: Function 7) */
	private FIBEntry getForwardingTableEntry(Integer packetId) {
		
		return forwardingTable.get(packetId);
	}
	
	/**
	 * Searches local cache and then global cache to find the the packet and if successful returns it else returns null;
	 * @param packetId Id if the data packet
	 * @return data packet
	 */
	/* Searches local storage and global cache (InterestPacketHander() comment: Function 2)*/
	private Packets getDataPacketfromCache(IDEntry dataObject) {
		
		/* Objects in local cache are represented by segment 0, hence, we have created the object below to compare against it.
		 * Essentially, we are ignoring the segment number in local caches to save on memory. The assumption is that all
		 * segments will be present so it is useless to explicitly store all of them
		 * */ 
		
		IDEntry localDataObject = new IDEntry (dataObject.getID(), 0);
		
		Packets packet = localStorage.get(localDataObject);
	
		if(packet != null) {
			
			return packet;
		}
		
		if (SimulationTypes.SIMULATION_CACHE == SimulationController.getCacheAvailability()) {
			
			packet = globalCache.get(dataObject);

			if(packet != null) {
				
				return packet;
			}
		}		
		return null;		
	}
	
	/**
	 * Floods packet on the all the interfaces of this router, except on the node from which this packet came from.
	 * @param curPacket Packet to be flooded.
	 */
	public void floodInterestPacket(Packets curPacket) {
		
		LinkedHashSet<HashMap<Integer, Integer>> adjList= Grid.getAdjacencyList(getRouterId());
		Iterator<HashMap<Integer,Integer>> itr = adjList.iterator();
		//log.info("Flooding the packet to {");

		/* Getting the previous node of the packet so as not to flood to same node */ 
		int srcNode = curPacket.getPrevHop(); 
		
		/* The following code is used to send packets to all the neighbors of a node */
		while(itr.hasNext()) {
			
			HashMap<Integer,Integer> adjNode = (HashMap<Integer, Integer>) itr.next();
			Integer nodeId = adjNode.keySet().iterator().next();
			
			 /* We shouldn't flood the interest packet to same node where it came from */
			if(nodeId != srcNode) {
				
				Packets pacToadd = (Packets)curPacket.clone();
				sendPacket(pacToadd, nodeId);
			}
		}
	}	
	
	/**
	 * This function puts the packet in the destination Router's queue. But makes necessary changes to the packet before putting it.
	 * It creates a new SimulationProcess TransmitPackets and adds transmission delay to it.
	 */
	public void sendPacket(Packets curPacket,final Integer nodeId) {
		
		if(nodeId > -1) {
			
			/* setting the source id as my Id before flooding it to my good neighbors */
			curPacket.setPrevHop(getRouterId()); 
			curPacket.incrHops();
			
			TransmitPackets trans = new TransmitPackets(curPacket, nodeId);
			
			try {
				trans.ActivateDelay(TransmitPackets.getTransDelay());
			} 
			catch (SimulationException e) {
				e.printStackTrace();
			} 
			catch (RestartException e) {
				e.printStackTrace();
			}
		}
		else {
			
			/* TODO: Implementation a termination condition that is based on the percentage of objects that have received.
			 *   The if-conditions comparing "countObjectsReceived" and getMaxSimulatedPackets() needs to be changed. 
			 *   Instead of the latter, we need to place 0.9 value. While for the former, we need to calculate what 
			 *   percentage of the total objects has been received.
			 *  */
		
			curPacket.finished(SupressionTypes.SUPRESSION_DEST_NODE);
			//log.info("Packet Destined to my node,so suppressing");
			
			/* Reality check values. These are printed towards the termination of simulation. */
			int numberOfHops = curPacket.getNoOfHops();
			
			switch (numberOfHops) {
			  case 0: 
			    hops0++;
			    break;
			  case 1: 
				  hops1++;
			    break;
			  case 2: 
				  hops2++;
			    break;
			  case 3: 
				  hops3++;
			    break;
			  case 4: 
				  hops4++;
			    break;
			  case 5: 
				  hops5++;
			    break;
			  case 6: 
				  hops6++;
			    break;
			  case 7: 
				  hops7++;
			    break;
			  default: 
				  hops8Plus++;
			}

			/* When a data packet has reached its destination, we search the TimeOutQueue to find out the corresponding interest packet. 
			 * We turn the receivedDataObject variable to true so that it does not retransmit */
			Iterator<TimeOutFields> listedTimeOutFields = SimulationController.timeOutQueue.iterator();	
			while(listedTimeOutFields.hasNext()) {
				
				TimeOutFields tof = listedTimeOutFields.next();
								
				if(tof.getInterestID() == curPacket.getRefPacketId() && tof.getSegmentID() == curPacket.getSegmentId() && tof.getObjectID() == curPacket.getPacketId() && tof.getNodeID() == this.getRouterId()) {
					tof.setReceivedDataObject(true);
					break;
				}
			}			
		}
	}
	
	@Override
	/* I am not sure if this function is even needed. Moreover, the two functions following it seem unnecessary */
	public void Activate() {
		
		if (!getPacketsQ().isEmpty() && !Processing()) {
			
			try {				
				super.Activate();
			}
			catch (SimulationException e) {
				//log.info("Exception "+ e.toString());
			}
			catch (RestartException e) {
				//log.info("Exception "+ e.toString());
			}
		}
		//else
			//log.info("Not activating "+getPacketsQ().isEmpty()+Processing());
	}	
	
	public boolean Processing () {
		return working;
	}
	
	@Override
	public String toString() {
		
		String str;
		if (SimulationTypes.SIMULATION_CACHE == SimulationController.getCacheAvailability()) {
			str = "CCNRouter\n{ Id:"+getRouterId()+ " \n"+ getPacketsQ().toString()+"\n PIT:"+getPIT().toString()+"\n ForwardingTable"+
			getForwardingTable().toString()+"\nGlobalCache:"+getGlobalCache().toString()+"\nLocalCache:"+getLocalStorage().toString()+"}\n";
		
		}
		else {
			str = "CCNRouter\n{ Id:"+getRouterId()+ " \n"+ getPacketsQ().toString()+"\n PIT:"+getPIT().toString()+"\n ForwardingTable"+
					getForwardingTable().toString() +"\nLocalCache:"+getLocalStorage().toString()+"}\n";
		}
		return str;
	}
	
	public CCNQueue getPacketsQ() {
		return packetsQ;
	}
	
	public void setPacketsQ(CCNQueue packetsQ) {
		packetsQ = this.packetsQ;
	}
	
	public int getRouterId() {
		return routerId;
	}
	
	public void setRouterId(int routerId) {
		this.routerId = routerId;
	}
	
	public boolean isPresentInPit(Integer packetId) {
		
		if(pit.containsKey(packetId))
			return true;
		else
			return false;
	}
	
	public boolean isPresentInQueue(Packets packet) {
		
	    if(packetsQ.contains(packet))
	    	return true;
	    else
	    	return false;
	}
	
	public Map<IDEntry, List<PITEntry>> getPIT() {
		return pit;
	}
	
	public void setPIT(Map<IDEntry, List<PITEntry>> tempPit) {
		pit = tempPit;
	}
	
	public CCNCache getLocalStorage() {
		return localStorage;
	}
	
	public void setLocalStorage(CCNCache tempLocalStorage) {
		this.localStorage = tempLocalStorage;
	}
	
	public CCNCache getGlobalCache() {
		return globalCache;
	}
	
	public void setGlobalCache(CCNCache globalCache) {
		this.globalCache = globalCache;
	}

	public Map<Integer, FIBEntry> getForwardingTable() {
		return forwardingTable;
	}

	public void setForwardingTable(Map<Integer, FIBEntry> forwardingTable) {
		this.forwardingTable = forwardingTable;
	}
	
	public BitSet[] getInterestsServed () {
		return interestServedBitArray;
	}

	public int getLogCounter() {
		
		logCounter=logCounter+1;
		return logCounter;
	}

	public void setLogCounter(int logCounter) {
		this.logCounter = logCounter;
	}

	public static double getProcDelay() {
		return procDelay;
	}

	public static void setProcDelay(double procDelay) {
		CCNRouter.procDelay = procDelay;
	}


	public int getDefaultInterface() {
		return defaultInterface;
	}

	public void decDefaultInterface() {
		defaultInterface--;
	}
};
