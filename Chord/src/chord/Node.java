package chord;

import java.util.concurrent.CopyOnWriteArrayList;
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
	
	private double stab_offset;
	private int stab_amplitude;
	
	private HashMap<Integer, String> data;
	
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
	public Node(Network<Object> viewNet, Random rnd, int hash_size, int id, double x, double y, double crash_pr, double recovery_interval, int successors_size, double stab_offset, int stab_amplitude) {
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
		this.setSuccessors(new CopyOnWriteArrayList<>());
		this.successors_size = successors_size;
		this.setPredecessor(null);
		this.next = 0;
		
		this.stab_offset = stab_offset;
		this.stab_amplitude = stab_amplitude+1;
		
		this.data = new HashMap<>();
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
				this.newData(successor.transferDataUpToKey(this.id));
				break;
			case "finger":
				this.finger.setEntry(position, successor);
				if(position == 1) {
					this.successors.set(0, successor);
				}
				break;
			case "successors":
				this.successors.set(position, successor);
				if(position == 0) {
					this.finger.setEntry(1, successor);
				}
				break;
			case "lookup":
				// call TopologyBuilder method
		}
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
	 * Processes a successor request
	 * @param id id of interest
	 * @return pair <Node, Boolean>: the first element is null if the current node is not subscribed or crashed; the second one defines if the retrieved node is the one responsible for the given id
	 */
	public Pair<Node, Boolean> processSuccRequest(int id) {
		Pair<Node, Boolean> pair = null;
		if(this.subscribed && !this.crashed) {
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
	 * Performs an iterative step of find_successor
	 * @param target_node node to ask for the given id
	 * @param info_source node from which the current node has become aware of target_node
	 * @param id id of interest
	 * @param target data structure: "init", "finger", "successors" or "lookup"
	 * @param position index in the target data structure
	 */
	private void find_successor_step(Node target_node, Node info_source, int id, String target_dt, int position) {
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
	 * Looks for the node responsible for the given identifier 
	 * @param id id of interest
	 * @param target_dt target data structure: "init", "finger", "successors" or "lookup"
	 * @param position index in the target data structure
	 */
	public void find_successor(int id, String target_dt, int position) {
		if (!this.successors.isEmpty()) {//TODO is this correct?
			if(Utils.belongsToInterval(id, this.id, this.successors.get(0).getId())) {
				setResult(this.successors.get(0), target_dt, position);
			} else {
				this.find_successor_step(this.closest_preceding_node(id), this, id, target_dt, position);
			}
		}
	}
	
	/**
	 * Creates a new Chord ring
	 */
	public void create() {
		this.setPredecessor(null);
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
		this.setPredecessor(null);
		this.find_successor_step(node, this, this.id, "init", 1);
		this.subscribed = true;
		
		this.schedule_stabilization();
	}
	
	/**
	 * Performs the initialization, setting the successor and scheduling the stabilization
	 * @param n successor node
	 */
	public void setSuccessor(Node successor) {
		this.setPredecessor(null);
		this.successors.add(successor);
		this.subscribed = true;
		
		this.schedule_stabilization();
	}
	
	
	/**
	 * Performs a stabilization step: stabilize, fix_fingers and check_predecessor are called
	 * @param retryCount: contacting the i-th successor, as the previous ones were inactive
	 * @throws RuntimeException if all successor have been contacted with no luck
	 */
	public void stabilization(int retryCount) {
		if(this.subscribed && !this.crashed) {
			Node suc = null;
			try {
				suc = this.successors.get(retryCount); 
			} catch (IndexOutOfBoundsException e) { //TODO issues to investigate
				//throw new RuntimeException("Node "+this.id+": Error! All successors are dead or disconnected, cannot stabilyze!");
				System.err.println("Node "+this.id+": Error! All successors are dead or disconnected, cannot stabilyze! "+this.successors);
			} 
			
			if (suc!=null) {
				double delay_req = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
				Pair<Node, CopyOnWriteArrayList<Node>> return_value = suc.processStabRequest(this,delay_req);
					
				double delay_resp = Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay);
				double delay_tot = return_value.getFirst() == null ? this.maximum_allowed_delay : delay_req+delay_resp;
				
				ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
				ScheduleParameters scheduleParameters = ScheduleParameters
						.createOneTime(schedule.getTickCount() + delay_tot/1000);
			
				if (return_value.getFirst() != null) {
					schedule.schedule(scheduleParameters, this, "processStabResponse", return_value);
				} else { //in this case the value is maximum_allowed_delay for sure, so it retries on timeout
					schedule.schedule(scheduleParameters, this, "stabilization", retryCount+1);		
				}
			}
			
			this.schedule_stabilization(); //schedule next stabilization
			
			this.fix_fingers();
			this.check_predecessor();
		}
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
			schedule.schedule(scheduleParameters, this, "setPredecessor", pred);
			
			return new Pair<Node, CopyOnWriteArrayList<Node>>(this,this.successors);
		} else {
			return new Pair<Node, CopyOnWriteArrayList<Node>>(null,null);
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
			updatedSucc.addAll(stabResponse.getSecond());	//attach its successors
			updatedSucc.remove(updatedSucc.size()-1);		//pop the last one
			
			this.setSuccessors(updatedSucc);
		} else {
			throw new RuntimeException("Error, impossible stabilization response from inactive node happened!");
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
			schedule.schedule(scheduleParameters, this, "stabilization",0);
		}
	}
	
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
				Node nonode = null;
				schedule.schedule(scheduleParameters, this, "setPredecessor", nonode);
			}
		}
	}
	
	public void fix_fingers() {
		if (this.next >= this.hash_size) {
			this.next = 0;
		}
		
		this.find_successor(this.id + (int) Math.pow(2, next-1), "finger", next);	

		this.next++;
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
	 * Performs the acquisition of data from another node
	 * @param data new data
	 */
	public void newData(HashMap<Integer, String> data) {
		this.data.putAll(data);
	}
	
	/**
	 * Performs the replacement of the predecessor and the acquisition of its data (in case it leaves the ring)
	 * @param new_predecessor reference to the new predecessor
	 * @param data new data
	 */
	public void replacePredecessorAndAcquireData(Node new_predecessor, HashMap<Integer, String> data) {
		this.setPredecessor(new_predecessor);
		this.newData(data);
	}
	
	/**
	 * Performs the reset of the predecessor and the acquisition of its data (in case it leaves the ring)
	 * @param data new data
	 */
	public void resetPredecessorAndAcquireData(HashMap<Integer, String> data) {
		this.setPredecessor(null);
		this.newData(data);
	}
	
	/**
	 * Performs the replacement of the last element in the successor list (in case the successor leaves)
	 * @param lastSuccessor the new last successor
	 */
	public void setLastSuccessor(Node lastSuccessor) {
		if(this.subscribed && !this.crashed) {
			this.successors.remove(0);
			this.successors.add(lastSuccessor);
			this.finger.setEntry(1, this.successors.get(0));
		}else {
			System.out.println("this node "+this.id+" is subscribed "+this.subscribed+" or crashed "+this.crashed);
		}
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
				schedule.schedule(scheduleParameters, successor, "replacePredecessorAndAcquireData", this.predecessor, this.data);
			} else {
				schedule.schedule(scheduleParameters, successor, "resetPredecessorAndAcquireData", this.data);
			}
		}
		
		if(!(this.predecessor == null)) {
			System.out.println("+++++++++++");
			this.debug();
			System.out.println("+++++++++++");
			ScheduleParameters scheduleParameters = ScheduleParameters
					.createOneTime(schedule.getTickCount() + Utils.getNextDelay(this.rnd, this.mean_packet_delay, this.maximum_allowed_delay));
			schedule.schedule(scheduleParameters, this.predecessor, "setLastSuccessor", this.successors.get(this.successors.size()-1));		
		}
		
		this.subscribed = false;
		
		this.finger.clearTable();
		this.successors.clear();
		this.setPredecessor(null);
		
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

	@Override
	public int compareTo(Node node) {
		return this.id.compareTo(node.getId());
	}
	
	public void debug() {
		System.out.println("\nNode id: "+this.id);
		System.out.println("\nSubscribe: "+this.subscribed);
		System.out.println("\nDown: "+this.crashed);
		System.out.println("Tick: "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
		System.out.println("Finger table:"+this.finger);
		String successors = "[";
		for(Node succ: this.successors) {
			successors += " "+succ.getId();
		}
		successors+= " ]";
		System.out.println("Successors: "+successors);
		System.out.println("Predecessor: "+this.predecessor);
		System.out.println("Data: "+this.data);
	}

	private void setSuccessors(CopyOnWriteArrayList<Node> successors) {
		this.successors = successors;
	}

	public void setPredecessor(Node predecessor) {
		if (predecessor!=null)
			System.out.println("Tick "+ RunEnvironment.getInstance().getCurrentSchedule().getTickCount() +", Node " +this.id.toString() + ": predecessor set "+predecessor.id.toString());
		this.predecessor = predecessor;
	}
}
