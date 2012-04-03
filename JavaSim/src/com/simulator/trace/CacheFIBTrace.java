package com.simulator.trace;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Formatter;

import com.simulator.packets.Packets;
import com.simulator.ccn.CCNRouter;
import com.simulator.controller.SimulationController;
import com.simulator.distributions.PacketDistributions;
import com.simulator.enums.PacketsType;
import com.simulator.enums.SimulationTypes;
import com.simulator.topology.Grid;

import arjuna.JavaSim.Simulation.*;;

/* The following class implements the logic of transmitting a packet. It is always called before sending a packet with a
 * delay, which represents the transmission delay. It is immediately terminated after implementing transmission which is
 * to simply add the packet into the queue of the destination node. The delay is implemented by calling this class with explicit
 * delay.
 * */
public class CacheFIBTrace extends SimulationProcess {
	
	private static double printDelay;
	
	//public static Writer fsCacheFIBTrace = null;
	
	public CacheFIBTrace () {
		
		try {
			
			Writer f = new BufferedWriter(new FileWriter("dump/cacheFIB.txt",true));
			System.out.println("\nInside Constructor: " + CCNRouter.CurrentTime());
		}		
	    catch (IOException e) {}
				
	}
	
	public void run() {
		
		for (;;) {
		    
			try {				
				
				StringBuilder str1 = new StringBuilder();
				Formatter str = new Formatter(str1);
				
				
				
				if (SimulationController.getDistributionType() == SimulationTypes.SIMULATION_DISTRIBUTION_LEAFNODE) {
					
					if (SimulationTypes.SIMULATION_CACHE == SimulationController.getCacheAvailability()) {
						
						str.format("%(,2.4f(c_leaf)\t",SimulationProcess.CurrentTime());
						//Integer noNodes = PacketDistributions.leafNodes.size();
											
						/* Tracing leaf nodes cache size */
						for (int i = 0; i < PacketDistributions.leafNodes.size(); i++) {
							str.format(" %d\t",Grid.getRouter(PacketDistributions.leafNodes.get(i)).getGlobalCache().usedEntries());
						}
						
						str.format("\n");
					}
						
					str.format("%(,2.4f(f_leaf)\t",SimulationProcess.CurrentTime());
					
					/* Tracing leaf nodes FIB size */
					for (int i = 0; i < PacketDistributions.leafNodes.size(); i++) {
						str.format(" %d\t",Grid.getRouter(PacketDistributions.leafNodes.get(i)).getForwardingTable().size());						
					}
					
					str.format("\n");
					
					if (SimulationTypes.SIMULATION_CACHE == SimulationController.getCacheAvailability()) { 
						str.format("%(,2.4f(c_core)",SimulationProcess.CurrentTime());
						
						/* Tracing core nodes cache size */
						for (int i = 0; i < Grid.getGridSize(); i++) {
							if (!PacketDistributions.leafNodes.contains(i)) {
								str.format(" %d\t",Grid.getRouter(i).getGlobalCache().usedEntries());
							}
						}
						
						str.format("\n");
					}
					
					str.format("%(,2.4f(f_core)\t",SimulationProcess.CurrentTime());
					
					/* Tracing core nodes FIB size */
					for (int i = 0; i < Grid.getGridSize(); i++) {
						if (!PacketDistributions.leafNodes.contains(i)) {
							str.format(" %d\t",Grid.getRouter(i).getForwardingTable().size());
						}
					}
														
					str.format("\n");
				}
				else {
					
					str.format("%(,2.4f(c)\t",SimulationProcess.CurrentTime());
					
					if (SimulationTypes.SIMULATION_CACHE == SimulationController.getCacheAvailability()) {
					
						/* Tracing Cache sizes of nodes */
						for (int i = 0; i < Grid.getGridSize(); i++) {
							str.format(" %d\t",Grid.getRouter(i).getGlobalCache().usedEntries());
						}
										
						str.format("\n");
					}
					
					str.format("%(,2.4f(f)\t",SimulationProcess.CurrentTime());
					
					/* Tracing FIB sizes of nodes */
					for (int i = 0; i < Grid.getGridSize(); i++) {
						str.format(" %d\t",Grid.getRouter(i).getForwardingTable().size());
					}			
					
					str.format("\n");
				}
				
				Writer f = new BufferedWriter(new FileWriter("dump/cacheFIB.txt",true));				
				f.write(str.toString());
				f.close();				
								
				Hold(50); 
				
			}	    
			catch (SimulationException e) {}			
			catch (RestartException e) {}			
			catch (IOException e) {}
		}
    }	
}