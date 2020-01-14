package chord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.RandomStringUtils;

import repast.simphony.context.Context;
import repast.simphony.context.space.continuous.ContinuousSpaceFactory;
import repast.simphony.context.space.continuous.ContinuousSpaceFactoryFinder;
import repast.simphony.context.space.graph.NetworkBuilder;
import repast.simphony.dataLoader.ContextBuilder;
import repast.simphony.engine.environment.RunEnvironment;
import repast.simphony.parameter.Parameters;
import repast.simphony.space.continuous.ContinuousSpace;
import repast.simphony.space.continuous.RandomCartesianAdder;
import repast.simphony.space.graph.Network;
import repast.simphony.util.collections.Pair;
import repast.simphony.engine.schedule.ISchedule;
import repast.simphony.engine.schedule.ScheduleParameters;

/**
 * This class loads all the parameters, initializes the Chord ring and the nodes and then schedules insertion/leaving/lookup batches 
 */
public class TopologyBuilder implements ContextBuilder<Object> {

	private final double end = 5000;
	private Random rnd;
	private ArrayList<Node> all_nodes;
	private TreeSet<Node> active_nodes;
	private int min_number_joins;
	private int join_amplitude;
	private int min_number_leaving;
	private int leaving_amplitude;
	private HashSet<Integer> keys;
	private ArrayList<Lookup> lookup_table;
	private double lookup_interval;
	private boolean one_key_lookup;
	private int number_lookup;
	private int forced_to_leave;
	private int additional_joins;
	
	/**
	 * Repast constructor: loads the simulation parameters; initializes the Chord ring and the nodes; generates the data and assigns them to the nodes; schedules leavings, insertions and lookups
	 * Two different initialization strategies can be chosen trough the one_at_time_init simulator parameter
	 * Two different lookups strategies can be chosen trough the one_key_lookup simulator parameter
	 * @param context context of repast
	 * @return the created context
	 */
	@Override
	public Context<Object> build(Context<Object> context) {
		Parameters params = RunEnvironment.getInstance().getParameters();
		
		int seed = params.getInteger("randomSeed");
		double crash_pr = params.getDouble("crash_pr");
		double crash_scheduling_interval = params.getDouble("crash_scheduling_interval");
		double recovery_interval = params.getDouble("recovery_interval");
		int succesors_size = params.getInteger("succesors_size");
		double stab_offset = params.getDouble("stab_offset");
		int stab_amplitude = params.getInteger("stab_amplitude");
		
		
		int hash_size = params.getInteger("m");
		int num_nodes = Double.valueOf(Math.pow(2, hash_size)).intValue();
		int space_size = num_nodes*4;
		int center = space_size/2;
		int radius = (center*3)/4;
		
		int init_num_nodes = params.getInteger("init_num_nodes");		
		boolean one_at_time_init = params.getBoolean("one_at_time_init");
		double insertion_delay = params.getDouble("insertion_delay")  > stab_offset+stab_amplitude ?  params.getDouble("insertion_delay") : stab_offset+stab_amplitude+1;
		
		int data_size = params.getInteger("data_size");
		int key_size = params.getInteger("key_size") > data_size ?  data_size : params.getInteger("key_size");
		int total_number_data = params.getInteger("total_number_data");
		
		double join_interval = params.getDouble("join_interval") > stab_offset+stab_amplitude ? params.getDouble("join_interval") : stab_offset+stab_amplitude+1; //the joins of new node MUST happens after at least a stab.
		this.min_number_joins = params.getInteger("min_number_joins");
		this.join_amplitude = params.getInteger("join_amplitude")+1; 
		
		double leave_interval = params.getDouble("leave_interval");
		this.min_number_leaving = params.getInteger("min_number_leaving");
		this.leaving_amplitude = params.getInteger("leaving_amplitude")+1; 
		
		this.lookup_interval = params.getDouble("lookup_interval");
		this.number_lookup = params.getInteger("number_lookup");
		this.one_key_lookup = params.getBoolean("one_key_lookup");
		
		context.setId("Chord");
		
		ContinuousSpaceFactory spaceFactory = ContinuousSpaceFactoryFinder.createContinuousSpaceFactory(null);
		ContinuousSpace<Object> space = spaceFactory.createContinuousSpace("space", context, new RandomCartesianAdder<Object>(),
				new repast.simphony.space.continuous.WrapAroundBorders(), space_size, space_size);
		
		NetworkBuilder<Object> netBuilder = new NetworkBuilder<Object>("chord_network", context, true);
		Network<Object> network = netBuilder.buildNetwork();

		Ring ring = new Ring(Float.valueOf(String.valueOf(radius)));
		context.add(ring);
		space.moveTo(ring, center, center);
		
		this.rnd = new Random(seed);
		this.lookup_table = new ArrayList<>();
		
		this.all_nodes = new ArrayList<>();
		this.forced_to_leave = 0;
		this.additional_joins = 0;
		
		for (int i = 0; i < num_nodes; i++) {
			Node node = new Node(
					this,
					network, 
					this.rnd, 
					hash_size, 
					i,
					center+radius*Math.sin(Math.toRadians((360.0/num_nodes)*i)), 
					center+radius*Math.cos(Math.toRadians((360.0/num_nodes)*i)),
					crash_pr,
					crash_scheduling_interval,
					recovery_interval,
					succesors_size,
					stab_offset,
					stab_amplitude,
					lookup_table
			);
			this.all_nodes.add(node);
		}
		this.keys = new HashSet<>();
		
		active_nodes = new TreeSet<>();
		
		if (one_at_time_init) {
			if (this.active_nodes.size() != init_num_nodes) {	
				ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
				ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+1);
				schedule.schedule(scheduleParams, this, "one_at_time_init", init_num_nodes, insertion_delay, context, space);
			}
		}else {
			preloaded_configuration(init_num_nodes, context, space);
		}
		
		
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		
		double data_gen = (one_at_time_init ? init_num_nodes*insertion_delay+(stab_offset+stab_amplitude) : (stab_offset+stab_amplitude));
		ScheduleParameters scheduleParamsDataGen = ScheduleParameters.createOneTime(data_gen);
		schedule.schedule(scheduleParamsDataGen, this, "data_generation", hash_size, key_size, data_size, total_number_data);
		
		double first_schedule = data_gen+this.lookup_interval;
		ScheduleParameters scheduleParamsLookup= ScheduleParameters.createRepeating(first_schedule, this.lookup_interval);
		if(this.one_key_lookup) {
			schedule.schedule(scheduleParamsLookup, this, "lookupSingleKey");
		}else {
			schedule.schedule(scheduleParamsLookup, this, "lookupMultipleKeys");
		}
		// the first batch of join has to be scheduled after the last node insert makes a stabilization and after the data generation, similar the first leave 
		double first_leave = (one_at_time_init ? init_num_nodes*insertion_delay+(stab_offset+stab_amplitude)+1 : (stab_offset+stab_amplitude)) + leave_interval+1;
		System.out.println("first leave "+first_leave + "  "+join_interval);

		ScheduleParameters scheduleParamsleave = ScheduleParameters.createRepeating(first_leave, leave_interval);
		

		schedule.schedule(scheduleParamsleave, this, "leaving_nodes", context, space, join_interval);
		
		ScheduleParameters scheduleParamsDebug = ScheduleParameters.createOneTime(10000);
		schedule.schedule(scheduleParamsDebug, this, "debug");

		ScheduleParameters scheduleLookup= ScheduleParameters.createOneTime(end);
		schedule.schedule(scheduleLookup, this, "getLookupsResults");
		
		return context;
	}
	
	/**
	 * Initialization strategy in which the nodes are inserted one at a time providing them with a random node already present the chord ring
	 * insertion_delay ticks are waited between two insertions in order to allow the new node to perform at least one stabilization
	 * @param init_num_nodes the number of nodes to initialize
	 * @param insertion_delay numbers of ticks between two insertions; it should be greater or equal than stab_offset+stab_amplitude
	 * @param context reference to the context
	 * @param space reference to the 2D space
	 */
	public void one_at_time_init(int init_num_nodes, double insertion_delay, Context<Object> context, ContinuousSpace<Object> space) {	
		Node node = this.all_nodes.get(this.rnd.nextInt(this.all_nodes.size()));
		while(this.active_nodes.contains(node)) {
			node = this.all_nodes.get(this.rnd.nextInt(this.all_nodes.size()));
		}
			
		this.active_nodes.add(node);
		context.add(node);
		space.moveTo(node, node.getX(), node.getY());
		if (this.active_nodes.size() == 1) {
			node.create();
		}else {
			Node succ_node = node;
			while (succ_node.equals(node) || succ_node.isCrashed()){
				succ_node = (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
			}
			System.out.println("joining "+node.getId());
			System.out.println("joining with "+succ_node.getId());
			System.out.println(this.active_nodes.size());
			node.join(succ_node);
		}
		
		if (this.active_nodes.size() != init_num_nodes) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+insertion_delay);
			schedule.schedule(scheduleParams, this, "one_at_time_init", init_num_nodes, insertion_delay, context, space);
		}
	}
	
	/**
	 * Initialization strategy in which init_num_nodes nodes are inserted concurrently providing them with the right immediate successor
	 * @param init_num_nodes the number of nodes to initialize
	 * @param context reference to the context
	 * @param space reference to the 2D space 
	 */
	private void preloaded_configuration(int init_num_nodes, Context<Object> context, ContinuousSpace<Object> space) {
		while(this.active_nodes.size() < init_num_nodes) {
			Node node = this.all_nodes.get(this.rnd.nextInt(this.all_nodes.size()));
			if(!this.active_nodes.contains(node)) {
				this.active_nodes.add(node);
				context.add(node);
				space.moveTo(node, node.getX(), node.getY());
			}
		}
		
		for( Node activeNode : this.active_nodes) {
			Node nextNode = this.active_nodes.higher(activeNode) != null ? this.active_nodes.higher(activeNode) : this.active_nodes.first();
			activeNode.initSuccessor(nextNode);
		}	
	}
	
	/**
	 * Creates total_number_data random strings that will be the data; each string is data_size characters long and 
	 * the first key_size characters are used as key 
	 * @param m hash size
	 * @param key_size number of characters used as key
	 * @param data_size lenght of the data
	 * @param total_number_data total number of data that will be generated
	 */
	public void data_generation(int m, int key_size, int data_size, int total_number_data) {
		while(this.keys.size() != total_number_data) {
			String data = RandomStringUtils.randomAlphabetic(data_size);
			String key = data.substring(0, key_size);
			Integer hashKey = Utils.getHash(key, m);
			if(!this.keys.contains(hashKey)) {
				this.keys.add(hashKey);
				HashMap<Integer, String> dataMap = new HashMap<>();
				dataMap.put(hashKey, data);
				Iterator<Node> it = this.active_nodes.iterator();
				Boolean find = false;
				while(it.hasNext() && !find) {
					Node node = it.next();
					if (node.getId() >= hashKey) {
						node.newData(dataMap);
						find = true;
					}
				}
				if(find == false) { //there is no node with an id greater than the hashKey so go to the first node
					this.active_nodes.first().newData(dataMap);
				}
			}
		}
	}
	
	/**
	 * Lookup strategy in which each node looks for a random key
	 */
	public void lookupMultipleKeys() {	
		HashSet<Node> lookupingNodes = new HashSet<>();
		HashSet<Node> validNodes = new HashSet<>();
		
		for(Node node: this.active_nodes) {
			if(node.isInitialized() && !node.isCrashed()) {
				validNodes.add(node);
			}
		}
		
		if (this.number_lookup >= validNodes.size()) {
			lookupingNodes.addAll(validNodes);
		}else {
			while(lookupingNodes.size() != this.number_lookup) {
				Node rndNode =  (new ArrayList<Node>(validNodes)).get(this.rnd.nextInt(validNodes.size()));
				if(!lookupingNodes.contains(rndNode) ) {
					lookupingNodes.add(rndNode);
				}
			}
		}
		
		for(Node node: lookupingNodes) {
			int hashKey = (new ArrayList<Integer>(this.keys)).get(this.rnd.nextInt(this.keys.size()));
			Lookup newLookup = new Lookup(this.lookup_table.size(), hashKey, node.getId(), RunEnvironment.getInstance().getCurrentSchedule().getTickCount(), this.firstNotCrashed(hashKey), this);
			this.lookup_table.add(newLookup);
			node.lookup(hashKey, this.lookup_table.size()-1);			
		}
	}
	
	/**
	 * Lookup strategy in which all nodes look for the same key (the key is different at every lookup batch)
	 */
	public void lookupSingleKey() {
		int hashKey = (new ArrayList<Integer>(this.keys)).get(this.rnd.nextInt(this.keys.size()));
		HashSet<Node> lookupingNodes = new HashSet<>();
		HashSet<Node> validNodes = new HashSet<>();
		
		for(Node node: this.active_nodes) {
			if(node.isInitialized() && !node.isCrashed()) {
				validNodes.add(node);
			}
		}	
		
		if (this.number_lookup >= validNodes.size()) {
			lookupingNodes.addAll(validNodes);
		}else {
			while(lookupingNodes.size() != this.number_lookup) {
				Node rndNode =  (new ArrayList<Node>(validNodes)).get(this.rnd.nextInt(validNodes.size()));
				if(!lookupingNodes.contains(rndNode) ) {
					lookupingNodes.add(rndNode);
				}
			}
		}
		
		for(Node node: lookupingNodes) {
			Lookup newLookup = new Lookup(this.lookup_table.size(), hashKey, node.getId(), RunEnvironment.getInstance().getCurrentSchedule().getTickCount(), this.firstNotCrashed(hashKey), this);
			this.lookup_table.add(newLookup);
			node.lookup(hashKey, this.lookup_table.size()-1);			
		}
	}
	
	/**
	 * Writes a CSV file with all the lookups performed
	 */
	public void getLookupsResults() {
		String results = "complete,duration,node_found,node_has_key,node_is_crashed,path_length,timeouts,nodes_contacted\n";
		for(Lookup entry: this.lookup_table) {
			results += entry.toCSV();
		}
		//return results;
        BufferedWriter writer = null;
        try {
            //create a temporary file
            String timeLog = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            File logFile = new File(timeLog+"_lookup.csv");
            // This will output the full path where the file will be written to...
            System.out.println("Lookup file saved in " + logFile.getCanonicalPath());
            writer = new BufferedWriter(new FileWriter(logFile));
            writer.write(results);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            }
        }
	}
	
	/**
	 * This method removes a variable number of nodes (between min_number_leave and this.min_number_leave + leave_amplitude) from the chord ring periodically;
	 * at least one node is left in the ring. 
	 * In order to be sure that the nodes in the ring are correct the first call is scheduled after stab_offset+stab_amplitude ticks since the last insertion in the initialization phase.
	 * Two consecutive leavings are separated by one tick.
	 * After the first call the method is scheduled every leave_interval.
	 * @param context reference to the context
	 * @param space reference to the continuous space
	 * @param join_interval number of ticks between the last leaving and the insertions
	 */
	public void leaving_nodes(Context<Object> context, ContinuousSpace<Object> space, double join_interval) {
		System.out.println("\nActive nodes before leaving: "+this.active_nodes.size());
		int exiting_nodes_number = this.min_number_leaving + this.rnd.nextInt(this.leaving_amplitude);
		exiting_nodes_number = exiting_nodes_number >= this.active_nodes.size() ? this.active_nodes.size() - 1 : exiting_nodes_number;
		HashSet<Node> leaving_nodes = new HashSet<>();
		while(leaving_nodes.size() != exiting_nodes_number) {
			Node rndNode =  (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
			if(!leaving_nodes.contains(rndNode) && rndNode.isInitialized() && !rndNode.isCrashed()) {
				leaving_nodes.add(rndNode);
			}
		}
		
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		
		int i = 0;
		for(Node n: leaving_nodes) {
			double t = schedule.getTickCount()+i;
			ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(t);
			schedule.schedule(scheduleParams, this, "nodeExit", context, n, leaving_nodes);
			i++;
		}
		
		System.out.println("Active nodes after leaving: "+this.active_nodes.size());
		
		double time = schedule.getTickCount()+i+join_interval;
		ScheduleParameters scheduleParamsJoin = ScheduleParameters.createOneTime(time);
		schedule.schedule(scheduleParamsJoin, this, "join_new_nodes", context, space);
		
		System.out.println("\n"+schedule.getTickCount()+" next join batch scheduled at "+ time);
	}
	
	/**
	 * Removes the node from the active ones and assign his data to the first greater node in the ring
	 * @param context reference to the context
	 * @param node node that has to leave the ring
	 * @param leaving_nodes set of nodes leaving the ring
	 */
	public void nodeExit(Context<Object> context, Node node, HashSet<Node> leaving_nodes) {
		SortedSet<Node> greaterNodes = this.active_nodes.tailSet(node, false);
		SortedSet<Node> smallerNodes = this.active_nodes.headSet(node, false);
		
		boolean nodeIsTheGreatest = true;
		boolean alredyFoundValidSucc = false;
		for(Node greaterNode: greaterNodes) {
			if(!leaving_nodes.contains(greaterNode) && !alredyFoundValidSucc) {
				greaterNode.newData(node.getData());
				node.leave();
				nodeIsTheGreatest = false;
				alredyFoundValidSucc = true;
				
			}
		}
		if(nodeIsTheGreatest) {
			alredyFoundValidSucc = false;
			for(Node smallerNode: smallerNodes) {
				if(!leaving_nodes.contains(smallerNode) && !alredyFoundValidSucc) {
					smallerNode.newData(node.getData());
					node.leave();
					alredyFoundValidSucc = true;
					
				}
			}
		}
		System.out.println("\nLeaving node "+node.getId()+"  "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
		this.active_nodes.remove(node);
	}
	
	/**
	 * Removes the specified node from the active ones and add an additional node to the next join batch
	 * @param node node that leaves the ring
	 */
	public void forced_to_leave(Node node) {
		System.out.println("\nForced leaving node "+node.getId()+"  "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
		this.active_nodes.remove(node);
		this.forced_to_leave++;
		this.additional_joins++;
	}
	
	/**
	 * This method inserts a variable number of nodes (between min_number_joins and this.min_number_joins + join_amplitude + additional_joins) in the chord ring periodically;
	 * if all the available nodes are already in the ring, no new nodes are inserted. 
	 * In order to ensure that the nodes in the ring are correct, the insertions are scheduled after stab_offset+stab_amplitude ticks since the last leaving
	 * @param context reference to the context
	 * @param space reference to the 2D space
	 */
	public void join_new_nodes(Context<Object> context, ContinuousSpace<Object> space) {
		int final_nodes_number = this.active_nodes.size() + this.min_number_joins + this.rnd.nextInt(this.join_amplitude) + this.additional_joins;
		this.additional_joins = 0;
		final_nodes_number  =  final_nodes_number > this.all_nodes.size() ? this.all_nodes.size() : final_nodes_number;
		HashSet<Integer> new_join_ids = new HashSet<>();
		while (this.active_nodes.size() != final_nodes_number ){
			Node rndNode =  (new ArrayList<Node>(this.all_nodes)).get(this.rnd.nextInt(this.all_nodes.size()));
			if (!this.active_nodes.contains(rndNode) && !new_join_ids.contains(rndNode.getId()) ) {
				this.active_nodes.add(rndNode);
				new_join_ids.add(rndNode.getId());
				context.add(rndNode);
				space.moveTo(rndNode, rndNode.getX(), rndNode.getY());
				Node succ_node = rndNode;
				while (succ_node.equals(rndNode) || new_join_ids.contains(succ_node.getId()) || succ_node.isCrashed()){
					succ_node = (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
				}

				System.out.println("\nJoining "+rndNode.getId()+ "  "+RunEnvironment.getInstance().getCurrentSchedule().getTickCount());
				System.out.println("Joining with "+succ_node.getId());
				rndNode.join(succ_node);
			}
		}
	}
	
	/**
	 * Returns the first initialized and not crashed node with an id greater or equal than the hash value provided
	 * @param key hash value of the key of interest
	 * @return id of the first non-crashed node with an id grater or equal than the hash value provided
	 */
	public Integer firstNotCrashed(Integer key) {
		boolean found = false;
		Node correctNode = null;
		Iterator<Node> it = this.active_nodes.iterator();
		while(it.hasNext() && !found) {
			Node node = it.next();
			if(!node.isCrashed() && node.isInitialized() && node.getId() >= key) {
				found = true;
				correctNode = node;
				
			}
		}
		if(!found) {
			it = this.active_nodes.iterator();
			while(it.hasNext() && !found) {
				Node node = it.next();
				if(!node.isCrashed() && node.isInitialized() && node.getId() < key) {
					found = true;
					correctNode = node;			
				}
			}
		}
		if(!found) {
			return null;
		} else {
			return correctNode.getId();
		}
	}
	
	/**
	 *  Return the number of nodes that have been forced to leave
	 * @return number of nodes that have been forced to leave
	 */
	public int getForcedToLeave() {
		return this.forced_to_leave;
	}
	
	/**
	 * Returns a pair of lists containing the missing successors and the wrong ones w.r.t. the ones provided
	 * @param node the node of interest
	 * @param successors the successors list of the node of interest
	 * @param hashSize the hash size
	 * @param max_succ_size the maximum size of the successors list
	 * @return a pair (ArrayList(Integer),ArrayList(Integer)) containing the missing successors and the wrong ones w.r.t. the ones provided
	 */
	public Pair<ArrayList<Integer>,ArrayList<Integer>> missingWrongSuccessors(Node node, ArrayList<Node> successors, Integer hashSize, Integer max_succ_size){
		ArrayList<Integer> rightSucc = new ArrayList<>();
		
		boolean end = false;
		Integer lastId = node.getId();
		while(rightSucc.size() < max_succ_size && !end) {
			Integer succId = this.firstNotCrashed((lastId+1) % (int)Math.pow(2, hashSize));
			if(succId != null && succId != node.getId() && !rightSucc.contains(succId)) {
				rightSucc.add(succId);
				lastId = succId;
			} else {
				end = true;
			}
		}
		if(rightSucc.isEmpty()) {
			rightSucc.add(lastId);
		}

		ArrayList<Integer> missingSucc = new ArrayList<>();
		ArrayList<Integer> wrongSucc = new ArrayList<>();
		
		int i=0;
		int j=0;
		while(i<successors.size()) {
			Node succ = successors.get(i);
			Integer succId = succ.getId();
			
			if(j < rightSucc.size()) {
				Integer rightSuccId = rightSucc.get(j);
				if(succId.equals(rightSuccId)) {
					i++;
					j++;
				} else if(rightSucc.contains(succId)) {
					missingSucc.add(rightSuccId);
					j++;
				} else {
					wrongSucc.add(succId);
					i++;
				}
			} else if (!succ.isSubscribed() || !succ.isInitialized() || succ.isCrashed()) {
				wrongSucc.add(succId);
				i++;
			} else {
				i++;
			}
		}
		
		return new Pair<ArrayList<Integer>,ArrayList<Integer>>(missingSucc,wrongSucc);
	}
	
	/**
	 * Prints some statistics about the lookups performed
	 */
	public void debug() {
		int correct = 0;
		int wrong = 0;
		int incomplete = 0;
		for(Lookup entry: this.lookup_table) {
			if(entry.isComplete()) {
				if(entry.getResult()) {
					correct++;
				} else {
					wrong++;
				}
			} else {
				incomplete ++;
			}
		}
		
		System.out.println("\nCorrect: "+correct);
		System.out.println("Wrong: "+wrong);
		System.out.println("Incomplete: "+incomplete+"\n");
		System.out.println("Forced leaving: "+ this.forced_to_leave);
	}
	
	/**
	 * Prints debug information about all active nodes
	 */
	public void printAll() {
		for(Node n: this.active_nodes) {
			n.debug();
		}
	}
}

