package chord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import repast.simphony.engine.environment.RunEnvironment;
import repast.simphony.engine.schedule.ISchedule;
import repast.simphony.engine.schedule.ScheduleParameters;
import repast.simphony.space.graph.Network;
import repast.simphony.space.graph.RepastEdge;
import repast.simphony.util.ContextUtils;
import repast.simphony.util.collections.Pair;

/**
 * This class defines the behavior of the agents in the simulation 
 */
public class Node implements Comparable<Node>{
	private TopologyBuilder top;
	private Network<Object> viewNet;
	private Random rnd;
	private int hash_size;
	private double mean_packet_delay = 50;
	private double maximum_allowed_delay = 500;

	private Integer id;
	private double x;
	private double y;
	
	private boolean initialized;
	private boolean subscribed;
	private boolean crashed;
	
	private double crash_pr;
	private double crash_scheduling_interval;
	private double recovery_interval;
	
	private FingerTable finger;
	private ArrayList<Node> successors;
	private int successors_size;
	private Node predecessor;
	
	private int next;
	private Node last_stabilized_succ;
	
	private double stab_offset;
	private int stab_amplitude;
	private boolean stabphase;
	
	private HashMap<Integer, String> data;
	private ArrayList<Lookup> lookup_table;
	
	/**
	 * Public constructor
	 * @param viewNet network
	 * @param rnd random number generator
	 * @param hash_size number of bits of the hash used for identifiers
	 * @param id node id
	 * @param x x coordinate in the continuous space
	 * @param y y coordinate in the continuous space
	 * @param crash_pr probability of node crash
	 * @param recovery_interval number of ticks needed for recovery
	 * @param successors_size size of the successors list
	 * @param stab_offset minimum offset between stabilizations
	 * @param stab_amplitude maximum interval to be added to the offset
	 */
	public Node(TopologyBuilder top, Network<Object> viewNet, Random rnd, int hash_size, int id, double x, double y, double crash_pr, double crash_scheduling_interval, double recovery_interval, int successors_size, double stab_offset, int stab_amplitude, ArrayList<Lookup> lookup_table) {
		this.top = top;
		
		this.viewNet = viewNet;
		this.rnd = rnd;
		this.hash_size = hash_size;
		
		this.id = id;
		this.x = x;
		this.y = y;
		
		this.initialized = false;
		this.subscribed = false;
		this.crashed = false;
		
		this.crash_pr = crash_pr;
		this.crash_scheduling_interval = crash_scheduling_interval;
		this.recovery_interval = recovery_interval;
		
		this.finger = new FingerTable(hash_size);
		this.successors = new ArrayList<>();
		this.successors_size = successors_size;
		this.resetPredecessor();
		
		this.next = 2;
		this.last_stabilized_succ = null;
		
		this.stab_offset = stab_offset;
		this.stab_amplitude = stab_amplitude+1;
		this.stabphase = true;
		
		this.data = new HashMap<>();
		this.lookup_table = lookup_table;
	}
	
	/**
	 * Creates a new Chord ring
	 */
	public void create() {
		this.resetPredecessor();
		this.finger.setEntry(1, this);
		this.successors.add(this);
		
		this.initialized = true;
		this.subscribed = true;
		
		this.schedule_stabilization();
		
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+this.crash_scheduling_interval);
		schedule.schedule(scheduleParams, this, "nodeCrash");
	}
	
	/**
	 * Joins an existing Chord ring
	 * @param node reference to a node already in the ring
	 */
	public void join(Node node) {
		this.resetPredecessor();
		ArrayList<Node> prev_contacted_nodes = new ArrayList<>();
		prev_contacted_nodes.add(this);
		this.find_successor_step(node, prev_contacted_nodes, this.id, "init", 1, 0, 0, 0);
		
		this.subscribed = true;
	}
	
	/**
	 * Performs the initialization, setting the successor and scheduling the stabilization
	 * @param n successor node
	 */
	public void initSuccessor(Node successor) {
		this.resetPredecessor();
		this.finger.setEntry(1, successor);
		this.successors.add(successor);
		this.subscribed = true;
		this.initialized = true;
		
		this.schedule_stabilization();
		
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+this.crash_scheduling_interval);
		schedule.schedule(scheduleParams, this, "nodeCrash");
	}
	
	/**
	 * Returns the closest preceding node w.r.t. the given id among the ones in finger and successors
	 * @param target_id id of interest
	 * @return closest preceding node
	 */
	public Node closest_preceding_node(int target_id) {
		Node candidate = null;
		
		ArrayList<Integer> finger_indices_desc = this.finger.getKeys(true);
		for(int i=0; i < finger_indices_desc.size() && candidate == null; i++) {
			int index = finger_indices_desc.get(i);
			int node_id = this.finger.getEntry(index).getId();
			if(Utils.belongsToInterval(node_id, this.id, target_id) && node_id != target_id) {
				candidate = this.finger.getEntry(index);
			}
		}
		
		if (candidate == null) {
			candidate = this;
		} else {
			boolean best_found = false;
			for(int j=this.successors.size()-1; j>=0 && !best_found; j--) {
				Node successor = this.successors.get(j);
				if(Utils.belongsToInterval(successor.getId(), candidate.getId(), target_id) && successor.getId() != target_id) {
					candidate = successor;
					best_found = true;
				}
			}
		}
		
		return candidate;
	}
	
	/**
	 * Looks for the node responsible for the given identifier 
	 * @param id id of interest
	 * @param target_dt target data structure: "init", "finger", "successors" or "lookup"
	 * @param position index in the target data structure
	 */
	public void find_successor(int id, String target_dt, int position) {
		if(Utils.belongsToInterval(id, this.id, this.successors.get(0).getId())) {
			setResult(this.successors.get(0), target_dt, position, 1, 0, 1);
		} else {
			ArrayList<Node> prev_contacted_nodes = new ArrayList<>();
			prev_contacted_nodes.add(this);
			this.find_successor_step(this.closest_preceding_node(id), prev_contacted_nodes, id, target_dt, position, 0, 0, 0);
		}
	}
	
	/**
	 * Performs an iterative step of find_successor
	 * @param target_node node to ask for the given id
	 * @param prev_contacted_nodes list of previously contacted nodes
	 * @param id id of interest
	 * @param target data structure: "init", "finger", "successors" or "lookup"
	 * @param position index in the target data structure
	 * @param path_length length of the query path
	 * @param nodes_contacted number of nodes contacted
	 */
	public void find_successor_step(Node target_node, ArrayList<Node> prev_contacted_nodes, int id, String target_dt, int position, int path_length, int num_timeouts, int nodes_contacted) {
		System.out.println("step "+this.id+ " -> "+target_node.getId()+" "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
		
		if(target_dt.equals("lookup")) { 
			this.removeOutEdges();
			this.viewNet.addEdge(this, target_node);
		}
		
		Pair<Node, Boolean> return_value = target_node.processSuccRequest(id);
		
		double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
		double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
		double delay_tot = return_value.getFirst() == null ? this.maximum_allowed_delay : delay_req+delay_resp;
		
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParameters = ScheduleParameters
				.createOneTime(schedule.getTickCount() + delay_tot/1000);
		schedule.schedule(scheduleParameters, this, "processSuccResponse", return_value, target_node, prev_contacted_nodes, id, target_dt, position, path_length, num_timeouts, nodes_contacted);
	}
	
	/**
	 * Processes a successor request
	 * @param id id of interest
	 * @return pair <Node, Boolean>: the first element is null if the current node is not subscribed or crashed; the second one defines if the retrieved node is the one responsible for the given id
	 */
	public Pair<Node, Boolean> processSuccRequest(int id) {
		Pair<Node, Boolean> pair = null;
		if(this.subscribed && this.initialized && !this.crashed) {
			if(Utils.belongsToInterval(id, this.id, this.successors.get(0).getId())) {
				pair = new Pair<Node, Boolean>(this.successors.get(0), true);
			} else {
				pair = new Pair<Node, Boolean>(this.closest_preceding_node(id), false);
			}
		} else {
			pair = new Pair<Node, Boolean>(null, false);
		}
		return pair;
	}
	
	/**
	 * Removes references to a node no longer present from finger and successor, returning the next closest preceding node w.r.t. the given id (if the current node is subscribed and not crashed).
	 * @param dead reference to the dead node
	 * @param id id of interest
	 * @return the reference to the next closest preceding node, null if the current node is unsubscribed or crashed
	 */
	public Node getPrevSuccessor(Node dead, int id) {
		if(this.subscribed && this.initialized && !this.crashed) {
			this.finger.removeEntry(dead);
			this.successors.remove(dead);
			if(!this.successors.isEmpty()) {
				this.finger.setEntry(1, this.successors.get(0));
			}
			
			if(this.finger.getEntry(1) == null) {
				return null;
			} else {
				return this.closest_preceding_node(id);
			}
		} else {
			return null;
		}
	}
	
	/**
	 * Processes response to a successor request
	 * @param response pair <Node, Boolean> returned by the contacted node
	 * @param source contacted node
	 * @param prev_info_source list of previously contacted nodes
	 * @param id id of interest
	 * @param target data structure: "init", "finger", "successors" or "lookup"
	 * @param position index in the target data structure
	 * @param path_length length of the query path
	 * @param nodes_contacted number of nodes contacted
	 */
	public void processSuccResponse(Pair<Node, Boolean> response, Node source, ArrayList<Node> prev_contacted_nodes, int id, String target_dt, int position, int path_length, int num_timeouts, int nodes_contacted) {
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		if(this.subscribed && !this.crashed) {
			if(response.getFirst() == null) {
				Node last_in_list = prev_contacted_nodes.get(prev_contacted_nodes.size()-1);
				if(target_dt.equals("lookup")) { 
					this.removeOutEdges();
					this.viewNet.addEdge(this, last_in_list);
				}
				Node prev_successor = last_in_list.getPrevSuccessor(source, id);
				
				if(prev_successor == null) {
					if(prev_contacted_nodes.size() == 1) {
						System.err.println("Error, no successor available for node "+last_in_list.getId()+"!");
						setResult(this, target_dt, position, -1, -1, -1);
					} else {
						Node dead = prev_contacted_nodes.remove(prev_contacted_nodes.size()-1);
						ScheduleParameters scheduleParameters = ScheduleParameters
								.createOneTime(schedule.getTickCount() + this.maximum_allowed_delay/1000);
						schedule.schedule(scheduleParameters, this, "processSuccResponse", new Pair<Node,Boolean>(null, false), dead, prev_contacted_nodes, id, target_dt, position, path_length-1, num_timeouts+1, nodes_contacted+1);
					}
				} else if (prev_successor.equals(this)){
					if(target_dt.equals("lookup")) { 
						this.removeOutEdges();
					}
					this.setResult(this.successors.get(0), target_dt, position, path_length+1, num_timeouts+1, nodes_contacted+2);
				} else if (last_in_list.equals(this)) {
					this.find_successor_step(prev_successor, prev_contacted_nodes, id, target_dt, position, path_length, num_timeouts+1, nodes_contacted+1);
				} else {					
					double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
					double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
					double delay_tot = delay_req+delay_resp;
					
					ScheduleParameters scheduleParameters = ScheduleParameters
							.createOneTime(schedule.getTickCount() + delay_tot/1000);
					schedule.schedule(scheduleParameters, this, "find_successor_step", prev_successor, prev_contacted_nodes, id, target_dt, position, path_length, num_timeouts+1, nodes_contacted+2);
				}
			} else {
				if(response.getSecond()) {
					if(target_dt.equals("lookup")) { 
						this.removeOutEdges();
					}
					this.setResult(response.getFirst(), target_dt, position, path_length+2, num_timeouts, nodes_contacted+2);
				} else {
					prev_contacted_nodes.add(source);
					this.find_successor_step(response.getFirst(), prev_contacted_nodes, id, target_dt, position, path_length+1, num_timeouts, nodes_contacted+1);
				}
			}
		} else {
			this.removeOutEdges();
		}
	}
	
	/**
	 * Inserts the node resulting from the execution of find_successor into the right data structure
	 * @param successor node responsible for the queried id
	 * @param target_dt target data structure: "init", "finger", "successors" or "lookup"
	 * @param position position index in the target data structure
	 * @param path_length length of the query path
	 * @param nodes_contacted number of nodes contacted
	 */
	private void setResult(Node successor, String target_dt, int position, int path_length, int num_timeouts, int nodes_contacted) {
		switch(target_dt) {
			case "init":
				if(!successor.equals(this)) {
					this.finger.setEntry(position, successor);
					this.successors.add(successor);
					this.stabilization(0);
				} else {
					this.forcedLeaving();
				}
				break;
			case "finger":
				if(position == 1) {
					this.finger.setEntry(position, successor);
					this.successors.set(0, successor);
				} else if (!successor.equals(this)) {
					this.finger.setEntry(position, successor);
					this.next++;
				} else {
					this.finger.removeEntry(position);
					this.next++;
				}
				break;
			case "successors":
				if(position == 0) {
					this.finger.setEntry(1, successor);
					if(this.successors.isEmpty()) {
						this.successors.add(successor);
					} else {
						this.successors.set(0, successor);
					}
				} else if (!successor.equals(this)) {
					int i = 0;
					while(i < this.successors.size() && Utils.belongsToInterval(this.successors.get(i).getId(), this.id, this.last_stabilized_succ.getId())) {
						i++;
					}
					position = i;
					
					this.last_stabilized_succ = successor;
					
					if(position >= this.successors.size()) {
						if(!this.successors.contains(successor)) {
							this.successors.add(successor);
						}
					} else {
						Node prev_element = this.successors.get(position);
						if(!prev_element.equals(successor)) {
							if(Utils.belongsToInterval(successor.getId(), this.successors.get(position-1).getId(), prev_element.getId())) {
								this.successors.add(position, successor);
								if(this.successors.size() > this.successors_size) {
									this.successors.remove(this.successors.size()-1);
								}
							} else {
								this.successors.set(position, successor);
								
								int j = position+1;
								boolean done = false;
								while(j < this.successors.size() && !done) {
									Node current = this.successors.get(j);
									if(Utils.belongsToInterval(current.getId(), this.successors.get(position-1).getId(), successor.getId())) {
										this.successors.remove(j);
									} else {
										done = true;
									}
								}
							}
						}
					}
					while(this.successors.size() > this.successors_size) {
						this.successors.remove(this.successors.size()-1);
					}
				} else {
					this.last_stabilized_succ = this.successors.get(0);
				}
				break;
			case "lookup":
				ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
				ScheduleParameters scheduleParameters = ScheduleParameters
						.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay)/1000);
				schedule.schedule(scheduleParameters, this.lookup_table.get(position), "setResult", successor, path_length, num_timeouts, nodes_contacted, Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay)/1000);
		}
	}
	
	/**
	 * Schedules the next stabilization step according to the given offset and amplitude
	 */
	public void schedule_stabilization() {
		if(this.subscribed) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			double scheduledTick = this.stab_offset + rnd.nextInt(this.stab_amplitude);
			System.out.println("\nTick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + ": scheduling stabilization at "+(schedule.getTickCount() + scheduledTick));
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + scheduledTick);
			schedule.schedule(scheduleParameters, this, "stabilization", 0);
		}
	}
	
	/**
	 * Performs a stabilization round: stabilization_step, fix_data_structures and check_predecessor are called
	 * @param retryCount: contacting the i-th successor, as the previous ones were inactive
	 * @throws RuntimeException if all successor have been contacted with no luck
	 */
	public void stabilization(int retryCount) {
		if(this.subscribed && !this.crashed) {
			boolean noMoreSucc = false;
			Node suc = null;
			try {
				suc = this.successors.get(retryCount); 
			} catch (IndexOutOfBoundsException e) { 
				System.out.println("Node "+this.id+": Error! All successors are dead or disconnected, cannot stabilize! "+this.successors);
				this.forcedLeaving();
				noMoreSucc = true;
			} 
			
			if (!noMoreSucc) {
				if(suc.equals(this)) {
					this.stabilization_step(this);
				} else {
					double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
					double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
					double delay_tot = (suc.crashed || !suc.subscribed) ? this.maximum_allowed_delay : delay_req+delay_resp;
					
					ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
					ScheduleParameters scheduleParameters = ScheduleParameters
							.createOneTime(schedule.getTickCount() + delay_tot/1000);
				
					if (suc.subscribed && !suc.crashed) {
						schedule.schedule(scheduleParameters, this, "stabilization_step", suc);
					} else { //in this case the value is maximum_allowed_delay for sure, so it retries on timeout
						schedule.schedule(scheduleParameters, this, "stabilization", retryCount+1);		
					}
				}
			}
		}
	}
	
	public void stabilization_step(Node answeringNode) {
		if(this.subscribed && !this.crashed) {
			//first time managing the step, add as first successor the predecessor of the node who answered		
			Node predecessorOfSuccessor = answeringNode.getPredecessor(); 
			//update successors
			System.out.println("stab "+this.id+ "  "+answeringNode.getId()+"  "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
			while (this.successors.get(0)!=answeringNode) {
				this.successors.remove(0);
			}
			
			if (predecessorOfSuccessor!=null && Utils.belongsToInterval(predecessorOfSuccessor.getId(), this.id, this.successors.get(0).getId()) && predecessorOfSuccessor.getId() != this.successors.get(0).getId()){
				this.successors.add(0,predecessorOfSuccessor);
				this.successors.remove(this);
				while(this.successors.size() > this.successors_size) {
					this.successors.remove(this.successors.size()-1);
				}
			}
			//update finger table
			this.finger.setEntry(1, successors.get(0));
			
			Node suc = this.successors.get(0); 
			if (suc!=null && !suc.equals(this)) {
				double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
				Pair<Node, ArrayList<Node>> return_value = suc.processStabRequest(this,delay_req);
					
				double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
				double delay_sum = delay_req+delay_resp;
				
				ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			
				if (return_value.getFirst() != null) {
					ScheduleParameters scheduleParameters = ScheduleParameters
							.createOneTime(schedule.getTickCount() + delay_sum/1000);
					schedule.schedule(scheduleParameters, this, "processStabResponse", return_value);
					this.schedule_stabilization(); //schedule next stabilization
				} else { //in this case the value is maximum_allowed_delay for sure, so it retries on timeout
					System.err.println("Node "+this.id+": SUCCESSOR is DEAD");
					ScheduleParameters scheduleParameters = ScheduleParameters
							.createOneTime(schedule.getTickCount() + delay_req/1000);
					schedule.schedule(scheduleParameters, answeringNode, "resetPredecessor");
					
					ScheduleParameters myScheduleParameters = ScheduleParameters
							.createOneTime(schedule.getTickCount() + this.maximum_allowed_delay/1000);
					schedule.schedule(myScheduleParameters, this, "stabilization", 1);		
				}
			} else {
				this.fix_data_structures();
				this.schedule_stabilization(); //schedule next stabilization
			}
		}
	}
	
	/**
	 * Returns the predecessor of the current node
	 * @return the predecessor of the current node
	 */
	public Node getPredecessor() {
		if(this.subscribed && !this.crashed) {
			return this.predecessor;
		}
		return null;
	}

	/**
	 * Responds to the stabilization request from another node
	 * @return a pair type containing the node itself and its successors, or (null,null) if not sub or crashed
	 */
	public Pair<Node, ArrayList<Node>> processStabRequest(Node pred, double set_pred_delay) {
		if(this.subscribed && this.initialized && !this.crashed) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + set_pred_delay/1000);
			schedule.schedule(scheduleParameters, this, "notifiedPredecessor", pred);
			
			return new Pair<Node, ArrayList<Node>>(this,this.successors);
		} else {
			System.err.println("Node "+this.id+": Sorry mate, I'm DEAD");
			return new Pair<Node, ArrayList<Node>>(null,null);
		}		
	}
	
	/**
	 * Updates the predecessor of the current node if the new one is closer w.r.t. the old one
	 * @param predecessor reference to the new predecessor
	 */
	public void notifiedPredecessor(Node predecessor) {
		System.out.println("\nTick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + ": predecessor set "+predecessor.id.toString());
		if(this.predecessor == null || (Utils.belongsToInterval(predecessor.getId(), this.predecessor.getId(), this.id) && predecessor.getId() != this.id)) {
			Node prev_predecessor = this.predecessor;
			this.predecessor = predecessor;
			
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			
			HashMap<Integer,String> dataToTransfer = this.transferDataUpToKey(this.predecessor.getId());
			if(!dataToTransfer.isEmpty()) {
				ScheduleParameters scheduleParameters = ScheduleParameters
						.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay)/1000);
				schedule.schedule(scheduleParameters, this.predecessor, "newData", dataToTransfer);
			}
				
			if(prev_predecessor != null) {
				ScheduleParameters scheduleParameters2 = ScheduleParameters
						.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay)/1000);
				schedule.schedule(scheduleParameters2, prev_predecessor, "setNewSuccessor", this.predecessor);
			}
		}
	}

	/**
	 * Sets the successor of the current node to the new value
	 * @param successor reference to the new successor
	 */
	public void setNewSuccessor(Node successor) {
		if(this.subscribed && !this.crashed) {
			this.finger.setEntry(1, successor);
			if(!this.successors.get(0).equals(successor)){
				if(this.successors.contains(successor)) {
					while(!this.successors.get(0).equals(successor)) {
						this.successors.remove(0);
					}
				} else {
					this.successors.add(0, successor);
					while(this.successors.size() > this.successors_size) {
						this.successors.remove(this.successors_size-1);
					}
				}
			}
		}
	}
	
	/**
	 * Manage the response of a stabilization request, updating the successors list, eventually asking the following successor in case the immediate ones is not available anymore
	 * @param pair of responding node and its successors list
	 */
	public void processStabResponse(Pair<Node, ArrayList<Node>> stabResponse) {
		if(this.subscribed && !this.crashed) {
			System.out.println("\nTick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + 
					": \n\treceived stabresponse from "+ stabResponse.getFirst().id.toString() + ": " + this.printableNodeList(stabResponse.getSecond()));
	
			if (stabResponse.getFirst() != null) {
				if(stabResponse.getFirst().equals(this.successors.get(0))) {
					ArrayList<Node> updatedSucc = new ArrayList<>();
					updatedSucc.add(stabResponse.getFirst());		//add the immediate successor
					
					boolean done = false;
					for(int i=0; i < stabResponse.getSecond().size() && !done; i++) { //attach its successors
						if(!stabResponse.getSecond().get(i).equals(this) && !stabResponse.getSecond().get(i).equals(updatedSucc.get(updatedSucc.size()-1))) {
							updatedSucc.add(stabResponse.getSecond().get(i));
						} else {
							done = true;
						}
					}
					
					if(updatedSucc.size() > this.successors_size) { //pop the last one
						updatedSucc.remove(updatedSucc.size()-1);
					}	
					this.successors = updatedSucc;
				}
					
				this.fix_data_structures();
			} else {
				throw new RuntimeException("Error, impossible stabilization response from inactive node happened!");
			}
		}
	}	
	
	/**
	 * Wrapper for the functions that manage the stabilization of the predecessor, the entry of the finger
	 * table and of the successors list
	 */
	public void fix_data_structures() {
		//alternate fix_fingers and fix_predecessor
		if (stabphase) {
			this.fix_fingers();
		} else {
			this.fix_successors();
		}
		this.stabphase = !this.stabphase;
		
		this.check_predecessor();
		
		if(!this.initialized) {
			this.initialized = true;
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+this.crash_scheduling_interval);
			schedule.schedule(scheduleParams, this, "nodeCrash");
		}
	}
	
	/**
	 * Stabilizes one entry of the finger table
	 */
	public void fix_fingers() {
		if (this.next > this.hash_size) {
			this.next = 2;
		}
		this.next = Math.min(next, this.finger.getFirstMissingKey());
		
		this.find_successor((this.id + (int) Math.pow(2, next-1)) %  ((int) Math.pow(2, this.hash_size)), "finger", next);	
	}
	
	/**
	 * Stabilizes one entry of the successors list
	 */
	public void fix_successors() {
		int index = -1;
		
		if (this.last_stabilized_succ == null || !this.successors.contains(this.last_stabilized_succ) || this.successors.indexOf(this.last_stabilized_succ) == this.successors_size-1) {
			this.last_stabilized_succ = this.successors.get(0);
			index = 0;
		} else {
			index = this.successors.indexOf(this.last_stabilized_succ);
		}
		
		this.find_successor((this.successors.get(index).getId()+1) % ((int) Math.pow(2, this.hash_size)), "successors", index+1);
	}
	
	/**
	 * Checks if the predecessor is still alive
	 */
	public void check_predecessor() {
		if (this.predecessor != null) {
			boolean down = (this.predecessor.crashed || !this.predecessor.subscribed);
			double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);			
			double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
			double delay_tot = down ? this.maximum_allowed_delay : delay_req+delay_resp;
			
			if (down) {
				System.out.println("Node "+this.id+": predecessor is down, scheduling its setting to null");
				ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
				ScheduleParameters scheduleParameters = ScheduleParameters
						.createOneTime(schedule.getTickCount() + delay_tot/1000);
				schedule.schedule(scheduleParameters, this, "resetPredecessor");
			}
		}
	}
	
	/**
	 * Performs the acquisition of data from another node
	 * @param data new data
	 */
	public void newData(HashMap<Integer, String> data) {
		this.data.putAll(data);
	}
	
	/**
	 * Provides the data up to a certain key
	 * @param target_key the id of interest
	 * @return the data up to the provided key
	 */
	public HashMap<Integer, String> transferDataUpToKey(int target_key){
		HashMap<Integer, String> dataToTransfer = new HashMap<>();
		
		ArrayList<Integer> keys = new ArrayList<Integer>(this.data.keySet());
		for(int i=0; i < keys.size(); i++) {
			int key = keys.get(i);
			if(Utils.belongsToInterval(key, this.id, target_key)) {
				dataToTransfer.put(key, this.data.get(key));
				this.data.remove(key);
			}
		}
		
		return dataToTransfer;
	}
	
	/**
	 * Makes the node crashing with a certain probability
	 */
	public void nodeCrash() {
		if(this.subscribed && !this.crashed) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			if(this.initialized && this.rnd.nextDouble() < this.crash_pr) {
				this.crashed = true;
				this.removeOutEdges();
				System.out.println("\nTick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + " is crashed");
				ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+this.recovery_interval);
				schedule.schedule(scheduleParams, this, "recovery");
			} else {
				ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+this.crash_scheduling_interval);
				schedule.schedule(scheduleParams, this, "nodeCrash");
			}
		}
	}
	
	/**
	 * Performs the recovery after a crash
	 */
	public void recovery() {
		this.crashed = false;
		System.out.println("\nTick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + " is up again");
		this.stabilization(0);
		
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+this.crash_scheduling_interval);
		schedule.schedule(scheduleParams, this, "nodeCrash");
	}
	
	/**
	 * Leaves the Chord ring, informing the successor and the predecessor
	 */
	public void leave() {
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		
		if(!successors.isEmpty()) {
			Node successor = this.successors.get(0);
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay));
			if(!(this.predecessor == null)) {
				schedule.schedule(scheduleParameters, successor, "setPredecessor", this.predecessor);
			} else {
				schedule.schedule(scheduleParameters, successor, "resetPredecessor");
			}
			if(!this.data.isEmpty()) {
				schedule.schedule(scheduleParameters, successor, "newData", this.data);
			}
		}
		
		if(!(this.predecessor == null)) {
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay));
			schedule.schedule(scheduleParameters, this.predecessor, "setLastSuccessor", this.successors.get(0), this.successors.get(this.successors.size()-1));		
		}
		
		this.crashed = false;
		this.initialized = false;
		this.subscribed = false;
		
		ScheduleParameters scheduleParameters = ScheduleParameters
				.createOneTime(schedule.getTickCount() + (this.maximum_allowed_delay+1)/1000);
		schedule.schedule(scheduleParameters, this, "clearAll");		
	}
	
	/**
	 * Resets the predecessor node
	 */
	public void resetPredecessor() {
		this.predecessor = null;
	}

	/**
	 * Sets the predecessor to the one provided if it is not equal to the current node
	 * @param predecessor reference to the new predecessor
	 */
	public void setPredecessor(Node predecessor) {
		this.predecessor = predecessor.equals(this) ? null : predecessor;
		
		if(this.predecessor != null) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay)/1000);
			schedule.schedule(scheduleParameters, this.predecessor, "setNewSuccessor", this);
		}
	}
	
	/**
	 * Performs the replacement of the last element in the successor list (in case the successor leaves)
	 * @param lastSuccessor the new last successor
	 */
	public void setLastSuccessor(Node firstSuccessor, Node lastSuccessor) {
		if(this.subscribed && this.initialized && !this.crashed) {
			Node successor = this.successors.get(0);
			
			if(!successor.equals(firstSuccessor)) {
				this.finger.removeEntry(successor);
				this.successors.remove(successor);
			}
			
			if(!lastSuccessor.equals(this)) {
				int index = this.successors.indexOf(lastSuccessor);
				if(index != -1) {
					this.successors = new ArrayList<>(this.successors.subList(0, index));
				} else {
					this.successors.add(lastSuccessor);
				}
				while(this.successors.size() > this.successors_size) {
					this.successors.remove(this.successors.size()-1);
				}
			}
			
			if(this.successors.isEmpty()) {
				this.finger.setEntry(1, this);
				this.successors.add(this);
			} else {
				this.finger.setEntry(1, this.successors.get(0));
			}
		}else {
			System.out.println("this node "+this.id+" is subscribed "+this.subscribed+" or crashed "+this.crashed);
		}
	}
	
	/**
	 * Clears all data structures when leaving the ring
	 */
	public void clearAll() {
		this.finger.clearTable();
		this.next = 2;
		
		this.successors.clear();
		this.last_stabilized_succ = null;
		this.resetPredecessor();
		
		this.stabphase = true;
		this.data.clear();
		
		this.removeOutEdges();
	}
	
	/**
	 * Performs a forced leave due to no successors available
	 */
	public void forcedLeaving() {
		if(!(this.predecessor == null)) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay)/1000);
			schedule.schedule(scheduleParameters, this.predecessor, "successorLeaving", this);
		}
		
		this.crashed = false;
		this.initialized = false;
		this.subscribed = false;
		
		this.clearAll();
		top.forced_to_leave(ContextUtils.getContext(this), this);
	}
	
	/**
	 * Removes a successor that is leaving
	 * @param successor reference to the leaving successor
	 */
	public void successorLeaving(Node successor) {
		if(this.subscribed && !this.crashed) {
			if(this.successors.get(0).equals(successor)) {
				this.successors.remove(0);
				if(!this.successors.isEmpty()) {
					this.finger.setEntry(1, this.successors.get(0));
				} else {
					this.forcedLeaving();
				}
			}
		}
	}
	
	/**
	 * Performs the lookup of a key in the ring
	 * @param key target key
	 * @param position index of the current lookup in the lookup_table
	 */
	public void lookup(int key, int position) {
		if(this.id == key) {
			this.setResult(this, "lookup", position, 0, 0, 0);
		} else {
			this.find_successor(key, "lookup", position);
		}
	}
	
	/**
	 * Removes the outgoing edges
	 */
	public void removeOutEdges() {
		Iterator<RepastEdge<Object>> iterator = this.viewNet.getOutEdges(this).iterator();
		while(iterator.hasNext()) {
			RepastEdge<Object> edge = iterator.next();
			this.viewNet.removeEdge(edge);
		}
	}
	
	/**
	 * Returns the hash size that the ids are based on
	 * @return the hash size that the ids are based on
	 */
	public int getHashSize() {
		return this.hash_size;
	}
	
	/**
	 * Returns the node id
	 * @return the node id
	 */
	public int getId() {
		return this.id;
	}
	
	/**
	 * Returns the node x coordinate in the continuous space
	 * @return the node x coordinate in the continuous space
	 */
	public double getX() {
		return x;
	}

	/**
	 * Returns the node y coordinate in the continuous space
	 * @return the node y coordinate in the continuous space
	 */
	public double getY() {
		return y;
	}
	
	/**
	 * Returns true if the node is crashed, false otherwise
	 * @return true if the node is crashed, false otherwise
	 */
	public boolean isCrashed() {
		return this.crashed;
	}
	
	/**
	 * Returns if the node has been initialized or not
	 * @return true is the node is initialized, false otherwise
	 */
	public boolean isInitialized() {
		return this.initialized;
	}
	
	/**
	 * Returns if the current node is subscribed to the ring
	 * @return true if it is subscribed, false otherwise
	 */
	public boolean isSubscribed() {
		return this.subscribed;
	}
	/**
	 * Returns the data managed by the current node
	 * @return the data managed by the current node
	 */
	public HashMap<Integer, String> getData() {
		return this.data;
	}
	
	/**
	 * Returns the size of the data managed by the current node
	 * @return the size of the data managed by the current node
	 */
	public Integer getDataSize() {
		return this.data.size();
	}
	
	/**
	 * Returns a string containing the missing successors and the wrong ones
	 * @return the missing and the wrong successors in string format
	 */
	public String getMissingWrongSuccessors() {
		Pair<ArrayList<Integer>,ArrayList<Integer>> data = this.top.missingWrongSuccessors(this, this.successors, this.hash_size, this.successors_size);
		return "("+data.getFirst().toString()+","+data.getSecond().toString()+")";
	}

	/**
	 * Returns a string containing the number of missing successors and wrong ones
	 * @return the number of missing successors and wrong ones in string format
	 */
	public String getMissingWrongSuccessorsNum() {
		Pair<ArrayList<Integer>,ArrayList<Integer>> data = this.top.missingWrongSuccessors(this, this.successors, this.hash_size, this.successors_size);
		return "("+data.getFirst().size()+","+data.getSecond().size()+")";
	}
	
	/**
	 * Returns the finger table of the current node
	 * @return the finger table of the current node
	 */
	public FingerTable getFinger() {
		return this.finger;
	}
	
	/**
	 * Returns the successors list of the current node
	 * @return the successors list of the current node
	 */
	public ArrayList<Node> getSuccessors() {
		return this.successors;
	}
	
	/**
	 * Debug function printing all node information
	 */
	public void debug() {
		System.out.println("\nNode id: "+this.id);
		System.out.println("Subscribed: "+this.subscribed);
		System.out.println("Initialized: "+this.initialized);
		System.out.println("Down: "+this.crashed);
		System.out.println("Tick: "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
		System.out.println("Finger table:"+this.finger);
		System.out.println("Successors: "+this.printableNodeList(this.successors));
		System.out.println("Predecessor: "+ (this.predecessor == null ? "null" : this.predecessor.getId()));
		System.out.println("Data: "+this.data);
	}
	
	/**
	 * Returns a printable version of a node list
	 * @param nodes list of nodes to print
	 * @return a printable version of the list
	 */
	public String printableNodeList(ArrayList<Node> nodes) {
		String successors_list = "[";
		for(Node succ: nodes) {
			successors_list += " "+succ.getId();
		}
		successors_list+= " ]";
		return successors_list;
	}
	
	@Override
	public int compareTo(Node node) {
		return this.id.compareTo(node.getId());
	}
}
