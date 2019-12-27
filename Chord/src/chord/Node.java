package chord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import repast.simphony.engine.environment.RunEnvironment;
import repast.simphony.engine.schedule.ISchedule;
import repast.simphony.engine.schedule.ScheduleParameters;
import repast.simphony.space.graph.Network;

public class Node {
	private Network<Object> viewNet;
	private Random rnd;
	private int hash_size;

	private Integer id;
	private double x;
	private double y;
	
	private boolean crashed = false;
	private double crash_pr;
	private double recovery_interval;
	
	private FingerTable finger;
	private ArrayList<Node> successors;
	private int successors_size;
	private Node predecessor;
	
	private double stab_offset;
	private int stab_amplitude;
	
	private HashMap<Integer, String> data;
	
	public Node(Network<Object> viewNet, Random rnd, int hash_size, int id, double x, double y, double crash_pr, double recovery_interval, int successors_size, double stab_offset, int stab_amplitude) {
		this.viewNet = viewNet;
		this.rnd = rnd;
		this.hash_size = hash_size;
		
		this.id = id;
		this.x = x;
		this.y = y;
		
		this.crash_pr = crash_pr;
		this.recovery_interval = recovery_interval;
		
		this.finger = new FingerTable(hash_size);
		this.successors = new ArrayList<>();
		this.successors_size = successors_size;
		this.predecessor = null;
		
		this.stab_offset = stab_offset;
		this.stab_amplitude = stab_amplitude+1;
		
		this.data = new HashMap<>();
	}
	
	
	public void create() {
		this.predecessor = null;
		this.successors.add(this);
		
		this.schedule_stabilization();
	}
	
	public void join(Node n) {
		this.predecessor = null;
	
		/*
		Node successor = n.find_successor(this.id);
		this.successors.add(successor);
		this.finger.setEntry(1, successor);
		*/
		
		this.schedule_stabilization();
	}
	
	
	/*
	 * stabilization => calls stabilize, fix_fingers, check_predecessor
	 */
	public void stabilization() {
		//TODO
	}
	
	
	public void transferData(HashMap<Integer, String> data) {
		this.data.putAll(data);
	}
	
	public void setPredecessor(Node new_predecessor) {
		this.predecessor = new_predecessor;
	}
	
	public void setLastSuccessor(Node lastSuccessor) {
		this.successors.remove(0);
		this.successors.add(lastSuccessor);
		this.finger.setEntry(1, this.successors.get(0));
	}
	
	public void leave() {
		if(!successors.isEmpty()) {
			Node successor = this.successors.get(0);
			successor.transferData(this.data);
			successor.setPredecessor(this.predecessor);
		}
		
		if(!(this.predecessor == null)) {
			this.predecessor.setLastSuccessor(this.successors.get(this.successors.size()-1));		
		}
		
		this.finger.clearTable();
		this.successors.clear();
		this.predecessor = null;
		
		this.data.clear();
	}
	
	
	public void schedule_stabilization() {
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParameters = ScheduleParameters
				.createOneTime(schedule.getTickCount() + this.stab_offset + rnd.nextInt(this.stab_amplitude));
		schedule.schedule(scheduleParameters, this, "stabilization");
	}
	
	
	public int getHashSize() {
		return this.hash_size;
	}
	
	public int getId() {
		return this.id;
	}
	
	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}
}
