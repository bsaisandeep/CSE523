package com.simulator.packets;


import java.io.*;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import org.apache.log4j.Logger;

import arjuna.JavaSim.Simulation.SimulationProcess;

import com.simulator.enums.PacketTypes;
import com.simulator.enums.SimulationTypes;
import com.simulator.enums.SupressionTypes;
import com.simulator.ccn.ObjectListEntry;
import com.simulator.controller.SimulationController;


public class PublishPacket extends Packets implements Cloneable {
	
	private List<ObjectListEntry> ObjectList = new ArrayList<ObjectListEntry>();
	
	private static int publishPacketId=0;
	
	private static String dataDumpFile="publishpacketsDump.txt";
	
public PublishPacket(Integer nodeId,int size, List<ObjectListEntry> routerObjectList) {
		
		setPacketId(getPublishPacketId());
		setSourcePacketId(getPacketId());
		setSegmentId (0);
		setPacketType(PacketTypes.PACKET_TYPE_PUBLISH);
		setPrevHop(-1);
		setRefPacketId(-1);
		setOriginNode(nodeId);
		setSizeOfPacket(size);
		setAlive(true);
		setObjectList(routerObjectList);
		setCauseOfSupr(SupressionTypes.SUPRESSION_NOT_APPLICABLE);
		//log.info("node id = "+nodeId+" packet id ="+ getPacketId());
	}

public synchronized static void publishdump(PublishPacket curPacket, String status) {

	try {
		@SuppressWarnings("unused")
		
		Writer fs = new BufferedWriter(new FileWriter(dataDumpFile,true));
		StringBuilder str1 = new StringBuilder();
		Formatter str = new Formatter(str1);
		
		str.format("%(,2.4f",SimulationProcess.CurrentTime());
		
		if(PacketTypes.PACKET_TYPE_PUBLISH == curPacket.getPacketType())
			str.format(" p");
				
		str.format(" %d",curPacket.getPacketId());
		str.format(" %d",curPacket.getSegmentId());
		str.format(" %s", status);
		str.format(" %d",curPacket.getRefPacketId());
		str.format(" %d",curPacket.getCurNode());
		str.format(" %d",curPacket.getPrevHop());
		str.format(" %d",curPacket.getOriginNode());
		str.format(" %d",curPacket.getNoOfHops());
		
		if(Integer.toBinaryString((curPacket.isAlive())?1:0).compareTo("1") == 0)
			str.format(" alive");
		else
			str.format(" dead");
		
		str.format(" %s", (curPacket.getCauseOfSupr().toString()));
		str.format(" No_cache");
		str.format(" %d", curPacket.getSizeOfPacket());
		str.format("\n");
		fs.write(str.toString());
		fs.close();			
		
		if (SimulationController.getDebugging() == SimulationTypes.SIMULATION_DEBUGGING_ON)
			publishcollectTrace (curPacket, status);
	}
	catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}		
}

public synchronized static void publishcollectTrace (PublishPacket curPacket, String status)	{

	try {
		@SuppressWarnings("unused")
		
		Writer fs = new BufferedWriter(new FileWriter("dump/publishReadableTrace.txt",true));
		StringBuilder str1 = new StringBuilder();
		Formatter str = new Formatter(str1);
		
		str.format("%(,2.4f\t",SimulationProcess.CurrentTime());
		
		if(PacketTypes.PACKET_TYPE_PUBLISH == curPacket.getPacketType())
			str.format("p\t");
		else 
			str.format("i\t");	
		
		str.format("id:%2d\t",curPacket.getPacketId());
		str.format("seg:%2d\t",curPacket.getSegmentId());
		str.format("status=%s\t", status);
		str.format("object/interest=%2d\t",curPacket.getRefPacketId());
		str.format("curr=%2d\t",curPacket.getCurNode());
		str.format("prev:%2d\t",curPacket.getPrevHop());
		str.format("src:%2d\t",curPacket.getOriginNode());
		str.format("hops=%d\t",curPacket.getNoOfHops());
		str.format("dead/alive="+ Integer.toBinaryString((curPacket.isAlive())?1:0) + "\t");
		str.format("%s\t", (curPacket.getCauseOfSupr().toString()));									
		str.format("No_cache \t");
		str.format("size= %d",curPacket.getSizeOfPacket());
		
		str.format("\n");
		fs.write(str.toString());
		fs.close();			
	}
	catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}		

}


public List<ObjectListEntry> getObjectList(){
	return ObjectList;
}

public void setObjectList(List<ObjectListEntry> tempObjectList){
	this.ObjectList =  tempObjectList;
}

public static void setDataDumpFile(String dataDumpFile) {
	PublishPacket.dataDumpFile = dataDumpFile;
}

public static synchronized Integer getPublishPacketId() {
	return publishPacketId++;
}

public static synchronized Integer getCurrentPublishPacketId() {
	return publishPacketId;
}

public static synchronized void setCurrenPacketId(Integer currenPacketId) {
	PublishPacket.publishPacketId = currenPacketId;
}

}
