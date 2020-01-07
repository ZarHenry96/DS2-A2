package chord;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import repast.simphony.engine.environment.RunEnvironment;
import repast.simphony.engine.schedule.ISchedule;
import repast.simphony.engine.schedule.ScheduleParameters;
import repast.simphony.space.graph.Network;
import repast.simphony.util.collections.Pair;

/**
 * This class defines the behavior of the agents in the simulation 
 */
public class Node implements Comparable<Node>{
	private Network<Object> viewNet;
	private Random rnd;
	private int hash_size;
	private double mean_packet_delay = 50;
	private double maximum_allowed_delay = 500;

	private Integer id;
	private double x;
	private double y;
	
	private boolean subscribed;
	private boolean crashed;
	private double crash_pr;
	private double recovery_interval;
	
	private FingerTable finger;
	private CopyOnWriteArrayList<Node> successors;
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
	public Node(Network<Object> viewNet, Random rnd, int hash_size, int id, double x, double y, double crash_pr, double recovery_interval, int successors_size, double stab_offset, int stab_amplitude, ArrayList<Lookup> lookup_table) {
		this.viewNet = viewNet;
		this.rnd = rnd;
		this.hash_size = hash_size;
		
		this.id = id;
		this.x = x;
		this.y = y;
		
		this.subscribed = false;
		this.crashed = false;
		
		this.crash_pr = crash_pr;
		this.recovery_interval = recovery_interval;
		
		this.finger = new FingerTable(hash_size);
		this.successors = new CopyOnWriteArrayList<>();
		this.successors_size = successors_size;
		this.resetPredecessor();
		
		this.next = 2;
		this.last_stabilized_succ = null;
		
		this.stab_offset = stab_offset;
		this.stab_amplitude = stab_amplitude+1;
		this.stabphase = false;
		
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
		this.subscribed = true;
		
		this.schedule_stabilization();
	}
	
	/**
	 * Joins an existing Chord ring
	 * @param node reference to a node already in the ring
	 */
	public void join(Node node) {
		this.resetPredecessor();
		this.find_successor_step(node, this, this.id, "init", 1);
		this.subscribed = true;
		
		this.schedule_stabilization();
	}
	
	/**
	 * Performs the initialization, setting the successor and scheduling the stabilization
	 * @param n successor node
	 */
	public void setSuccessor(Node successor) {
		this.resetPredecessor();
		this.successors.add(successor);
		this.subscribed = true;
		
		this.schedule_stabilization();
	}
	
	/**
	 * Returns the closest preceding node w.r.t. the given id among the ones in finger and successors
	 * @param target_id id of interest
	 * @return closest preceding node
	 */
	private Node closest_preceding_node(int target_id) {
		Node candidate = null;
		
		CopyOnWriteArrayList<Integer> finger_indices_desc = this.finger.getKeys(true);
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
			setResult(this.successors.get(0), target_dt, position);
		} else {
			this.find_successor_step(this.closest_preceding_node(id), this, id, target_dt, position);
		}
	}
	
	/**
	 * Performs an iterative step of find_successor
	 * @param target_node node to ask for the given id
	 * @param info_source node from which the current node has become aware of target_node
	 * @param id id of interest
	 * @param target data structure: "init", "finger", "successors" or "lookup"
	 * @param position index in the target data structure
	 */
	public void find_successor_step(Node target_node, Node info_source, int id, String target_dt, int position) {

		System.err.println("step -> "+this.id+ "  "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
		Pair<Node, Boolean> return_value = target_node.processSuccRequest(id);
		
		double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
		double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
		double delay_tot = return_value.getFirst() == null ? this.maximum_allowed_delay : delay_req+delay_resp;
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParameters = ScheduleParameters
				.createOneTime(schedule.getTickCount() + delay_tot/1000);
		schedule.schedule(scheduleParameters, this, "processSuccResponse", return_value, target_node, info_source, id, target_dt, position);
	}
	
	/**
	 * Processes a successor request
	 * @param id id of interest
	 * @return pair <Node, Boolean>: the first element is null if the current node is not subscribed or crashed; the second one defines if the retrieved node is the one responsible for the given id
	 */
	public Pair<Node, Boolean> processSuccRequest(int id) {
		Pair<Node, Boolean> pair = null;
		if(this.subscribed && !this.crashed) {
			System.out.println("step <- "+this.id+ "  "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
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
	 * Removes references to a node no longer present from finger and successor, returning the next closest preceding node w.r.t. the given id
	 * @param dead 
	 * @param id id of interest
	 * @return
	 */
	public Node getPrevSuccessor(Node dead, int id) {
		this.finger.removeEntry(dead);
		this.successors.remove(dead);
		this.debug();
		
		return this.closest_preceding_node(id);
	}
	
	/**
	 * Processes response to a successor request
	 * @param response pair <Node, Boolean> returned by the contacted node
	 * @param info_source contacted node
	 * @param prev_info_source node from which the current node has become aware of info_source
	 * @param id id of interest
	 * @param target data structure: "init", "finger", "successors" or "lookup"
	 * @param position index in the target data structure
	 */
	public void processSuccResponse(Pair<Node, Boolean> response, Node info_source, Node prev_info_source, int id, String target_dt, int position) {
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		if(this.subscribed && !this.crashed) {
			if(response.getFirst() == null) {
				Node prev_successor = prev_info_source.getPrevSuccessor(info_source, id);
				if(!prev_successor.equals(prev_info_source)) {
					double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
					double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
					double delay_tot = delay_req+delay_resp;
					
					ScheduleParameters scheduleParameters = ScheduleParameters
							.createOneTime(schedule.getTickCount() + delay_tot/1000);
					schedule.schedule(scheduleParameters, this, "find_successor_step", prev_successor, prev_info_source, id, target_dt, position);
				} else {
					//throw new RuntimeException("Error, no successor available for node "+prev_info_source.getId()+"!");
					System.err.println("Error, no successor available for node "+prev_info_source.getId()+"!");
				}
			} else {
				if(response.getSecond()) {
					this.setResult(response.getFirst(), target_dt, position);
				} else {
					this.find_successor_step(response.getFirst(), info_source, id, target_dt, position);
				}
			}
		}
	}
	
	/**
	 * Inserts the node resulting from the execution of find_successor into the right data structure
	 * @param successor node responsible for the queried id
	 * @param target_dt target data structure: "init", "finger", "successors" or "lookup"
	 * @param position position index in the target data structure
	 */
	private void setResult(Node successor, String target_dt, int position) {
		switch(target_dt) {
			case "init":
				this.finger.setEntry(position, successor);
				this.successors.add(successor);
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
				} else if (!successor.equals(this)){
					this.last_stabilized_succ = successor;
					
					if(position >= this.successors.size()) {
						if(!this.successors.get(this.successors.size()-1).equals(successor)) {
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
								
								int i = position+1;
								boolean done = false;
								while(i < this.successors.size() && !done) {
									Node current = this.successors.get(i);
									if(Utils.belongsToInterval(current.getId(), prev_element.getId(), successor.getId())) {
										this.successors.remove(i);
									} else {
										done = true;
									}
								}
							}
						}
					}
				} else {
					this.last_stabilized_succ = this.successors.get(0);
				}
				break;
			case "lookup":
				// call TopologyBuilder method
		}
	}
	
	/**
	 * Schedules the next stabilization step according to the given offset and amplitude
	 */
	public void schedule_stabilization() {
		if(this.subscribed && !this.crashed) {
			this.debug();
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			double scheduledTick = this.stab_offset + rnd.nextInt(this.stab_amplitude);
			System.out.println("Tick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + ": scheduling stabilization at "+(schedule.getTickCount() + scheduledTick));
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
				//throw new RuntimeException("Node "+this.id+": Error! All successors are dead or disconnected, cannot stabilize!");
				System.out.println("Node "+this.id+": Error! All successors are dead or disconnected, cannot stabilize! "+this.successors);
				this.finger.setEntry(1, this);
				this.successors.clear();
				this.successors.add(this);
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
				
					if (!suc.crashed && suc.subscribed) {
						schedule.schedule(scheduleParameters, this, "stabilization_step", suc);
					} else { //in this case the value is maximum_allowed_delay for sure, so it retries on timeout
						schedule.schedule(scheduleParameters, this, "stabilization", retryCount+1);		
					}
				}
			} else {
				this.schedule_stabilization(); //schedule next stabilization
			}
		}
	}
	
	public void stabilization_step(Node answeringNode) {
		if(this.subscribed && !this.crashed) {
			//first time managing the step, add as first successor the predecessor of the node who answered		
			Node predecessorOfSuccessor = answeringNode.getPredecessor(); 
			//update successors
			System.err.println("stab "+this.id+ "  "+answeringNode.getId()+"  "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
			while (this.successors.get(0)!=answeringNode) {
				this.successors.remove(0);
			}
			
			if (predecessorOfSuccessor!=null && Utils.belongsToInterval(predecessorOfSuccessor.getId(), this.id, this.successors.get(0).getId()) && predecessorOfSuccessor.getId() != this.successors.get(0).getId()){
				this.successors.add(0,predecessorOfSuccessor);
				this.successors.remove(this);
				if(this.successors.size() > this.successors_size) {
					this.successors.remove(this.successors.size()-1);
				}
			}
			//update finger table
			this.finger.setEntry(1, successors.get(0));
			
			Node suc = this.successors.get(0); 
			if (suc!=null && !suc.equals(this)) {
				double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
				Pair<Node, CopyOnWriteArrayList<Node>> return_value = suc.processStabRequest(this,delay_req);
					
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
	public Pair<Node, CopyOnWriteArrayList<Node>> processStabRequest(Node pred, double set_pred_delay) {
		if(this.subscribed && !this.crashed) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + set_pred_delay/1000);
			schedule.schedule(scheduleParameters, this, "notifyPredecessor", pred);
			
			this.debug();
			return new Pair<Node, CopyOnWriteArrayList<Node>>(this,this.successors);
		} else {
			System.err.println("Node "+this.id+": Sorry mate, I'm DEAD");
			return new Pair<Node, CopyOnWriteArrayList<Node>>(null,null);
		}		
	}
	
	/**
	 * Updates the predecessor of the current node if the new one is closer w.r.t. the old one
	 * @param predecessor reference to the new predecessor
	 */
	public void notifyPredecessor(Node predecessor) {
		System.out.println("Tick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + ": predecessor set "+predecessor.id.toString());
		if(this.predecessor == null || (Utils.belongsToInterval(predecessor.getId(), this.predecessor.getId(), this.id) && predecessor.getId() != this.id)) {
			this.predecessor = predecessor;
			
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay)/1000);
			schedule.schedule(scheduleParameters, this.predecessor, "newData", this.transferDataUpToKey(this.predecessor.getId()));
		}
	}

	/**
	 * Manage the response of a stabilization request, updating the successors list, eventually asking the following successor in case the immediate ones is not available anymore
	 * @param pair of responding node and its successors list
	 */
	public void processStabResponse(Pair<Node, CopyOnWriteArrayList<Node>> stabResponse) {
		System.out.println("Tick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + 
				": \n\treceived stabresponse from "+ stabResponse.getFirst().id.toString() + ": " + stabResponse.getSecond().toString());

		if (stabResponse.getFirst() != null) {
			CopyOnWriteArrayList<Node> updatedSucc = new CopyOnWriteArrayList<>();
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
			
			this.fix_data_structures();
		} else {
			throw new RuntimeException("Error, impossible stabilization response from inactive node happened!");
		}
	}	
	
	/**
	 * Wrapper for the functions
	 */
	public void fix_data_structures() {
		//alternate fix_fingers and check_predecessor
		if (stabphase) {
			this.fix_fingers();
		} else {
			this.fix_successors();
		}
		this.check_predecessor();
		this.stabphase = !this.stabphase;
	}
	
	/**
	 * Stabilizes one entry of the finger table
	 */
	public void fix_fingers() {
		if (this.next > this.hash_size) {
			this.next = 2;
		}
		
		this.find_successor((this.id + (int) Math.pow(2, next-1)) %  ((int) Math.pow(2, this.hash_size)), "finger", next);	
	}
	
	/**
	 * Stabilizes one entry of the successors list
	 */
	public void fix_successors() {
		if (this.last_stabilized_succ == null || !this.successors.contains(this.last_stabilized_succ)) {
			this.last_stabilized_succ = this.successors.get(0);
		}
		
		int index = this.successors.indexOf(this.last_stabilized_succ);
		if (index == this.successors_size-1) {
			index = 0;
		}
		
		this.find_successor(this.successors.get(index).getId()+1, "successors", index+1);
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
		
		CopyOnWriteArrayList<Integer> sortedKeys = new CopyOnWriteArrayList<Integer>(this.data.keySet());
		Collections.sort(sortedKeys);
		
		boolean done = false;
		for(int i=0; i < sortedKeys.size() && !done; i++) {
			int key = sortedKeys.get(i);
			if(key <= target_key) {
				dataToTransfer.put(key, this.data.get(key));
				this.data.remove(key);
			} else {
				done = true;
			}
		}
		
		return dataToTransfer;
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
			this.debug();
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay));
			schedule.schedule(scheduleParameters, this.predecessor, "setLastSuccessor", this.successors.get(this.successors.size()-1));		
		}
		
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
	}
	
	/**
	 * Performs the replacement of the last element in the successor list (in case the successor leaves)
	 * @param lastSuccessor the new last successor
	 */
	public void setLastSuccessor(Node lastSuccessor) {
		if(this.subscribed && !this.crashed) {
			this.successors.remove(0);
			if(!lastSuccessor.equals(this)) {
				this.successors.add(lastSuccessor);
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
		this.successors.clear();
		this.resetPredecessor();
		
		this.data.clear();
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

	public void debug() {
		System.out.println("\nNode id: "+this.id);
		System.out.println("\nSubscribed: "+this.subscribed);
		System.out.println("\nDown: "+this.crashed);
		System.out.println("Tick: "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
		System.out.println("Finger table:"+this.finger);
		String successors = "[";
		for(Node succ: this.successors) {
			successors += " "+succ.getId();
		}
		successors+= " ]";
		System.out.println("Successors: "+successors);
		System.out.println("Predecessor: "+ (this.predecessor == null ? "null" : this.predecessor.getId()));
		System.out.println("Data: "+this.data);
	}
	
	@Override
	public int compareTo(Node node) {
		return this.id.compareTo(node.getId());
	}
}
