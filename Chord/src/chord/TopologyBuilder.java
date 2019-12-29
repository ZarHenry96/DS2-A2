package chord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.lang3.RandomStringUtils;

import net.sf.jasperreports.engine.util.DigestUtils;
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
import repast.simphony.engine.schedule.ISchedule;
import repast.simphony.engine.schedule.ScheduleParameters;
/**
 * This class loads all the parameters, after that initialises the chord ring and nodes, finally it schedules joins and leaves of batch of nodes from the chord ring. 
 */
public class TopologyBuilder implements ContextBuilder<Object> {

	private Random rnd;
	private ArrayList<Node> all_nodes;
	private TreeSet<Node> active_nodes;
	private int min_number_joins;
	private int join_amplitude;
	private int min_number_leaving;
	private int leaving_amplitude;
	
	
	/**
	 * Repast constructor loads the simulation parameters, init nodes and chord ring and finally schedules joins and leaves. 
	 * Two different initialization strategy can be choose trough the one_at_time_init simulations params.
	 * @param context context of repast
	 * @return context
	 */
	@Override
	public Context<Object> build(Context<Object> context) {
		Parameters params = RunEnvironment.getInstance().getParameters();
		
		int seed = params.getInteger("randomSeed");
		double crash_pr = params.getDouble("crash_pr");
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
		double insertion_delay = params.getDouble("insertion_delay");
		
		double join_interval = params.getDouble("join_interval");
		this.min_number_joins = params.getInteger("min_number_joins");
		this.join_amplitude = params.getInteger("join_amplitude")+1; 
		
		double leave_interval = params.getDouble("leave_interval");
		this.min_number_leaving = params.getInteger("min_number_leaving");
		this.leaving_amplitude = params.getInteger("leaving_amplitude")+1; 
		
		Utils.setMaxId(hash_size);
		
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
		this.all_nodes = new ArrayList<>();
		for (int i = 0; i < num_nodes; i++) {
			
			Node node = new Node(
					network, 
					this.rnd, 
					hash_size, 
					i,
					center+radius*Math.sin(Math.toRadians((360.0/num_nodes)*i)), 
					center+radius*Math.cos(Math.toRadians((360.0/num_nodes)*i)),
					crash_pr, 
					recovery_interval,
					succesors_size,
					stab_offset,
					stab_amplitude
			);
			this.all_nodes.add(node);
			
		}
		
		active_nodes = new TreeSet<>();
		
		if (one_at_time_init) {
			if (this.active_nodes.size() != init_num_nodes) {
				ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
				ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+insertion_delay);
				schedule.schedule(scheduleParams, this, "one_at_time_init", init_num_nodes, insertion_delay, context, space);
			}
		}else {
			preloaded_configuration(init_num_nodes, context, space);
		}
		
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		
		// the first batch of join has to be scheduled after the last node insert makes a stabilization, similar the first leave 
		double first_join = (one_at_time_init ? init_num_nodes*insertion_delay+(stab_offset+stab_amplitude) : (stab_offset+stab_amplitude)) + join_interval;
		double first_leave = (one_at_time_init ? init_num_nodes*insertion_delay+(stab_offset+stab_amplitude) : (stab_offset+stab_amplitude)) + leave_interval;
		System.out.println(first_join);
		System.out.println(first_leave);
		ScheduleParameters scheduleParamsJoin = ScheduleParameters.createRepeating(first_join, join_interval);
		ScheduleParameters scheduleParamsleave = ScheduleParameters.createRepeating(first_leave, leave_interval);
		
		schedule.schedule(scheduleParamsJoin, this, "join_new_nodes", context, space);
		schedule.schedule(scheduleParamsleave, this, "leaving_nodes", context, space);
		/*
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParams = ScheduleParameters.createRepeating(start, interval);
		schedule.schedule(scheduleParams, this, "");
		*/
		
		return context;
	}
	
	/**
	 * Initialization strategy where each node is inserted at time, giving a random node in the chord ring, and leaving it to find is correct successor.
	 * this method must be public as the method is scheduled to add one node each insertion_delay steps, in order to allow the new node to perform at least one stabilization.
	 * @param init_num_nodes the requested number of nodes that have to be initialized 
	 * @param insertion_delay numbers of ticks between two insertion, should be greater or equals than stab_offset+stab_amplitude
	 * @param context the context where add nodes
	 * @param space the 2D space where add nodes
	 */
	public void one_at_time_init(int init_num_nodes, double insertion_delay, Context<Object> context, ContinuousSpace<Object> space) {
		
		Node node = this.all_nodes.get(this.rnd.nextInt(this.all_nodes.size()));
		if(!this.active_nodes.contains(node)) {
			this.active_nodes.add(node);
			context.add(node);
			space.moveTo(node, node.getX(), node.getY());
			if (this.active_nodes.size() == 1) {
				node.create();
			}else {
				Node succ_node = node;
				while (succ_node.equals(node)){
					succ_node = (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
				}
				node.join(succ_node);
			}
		}
		if (this.active_nodes.size() != init_num_nodes) {
			ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
			ScheduleParameters scheduleParams = ScheduleParameters.createOneTime(schedule.getTickCount()+insertion_delay);
			schedule.schedule(scheduleParams, this, "one_at_time_init", init_num_nodes, insertion_delay, context, space);
		}
		
	}
	
	/**
	 * Initialization strategy where init_num_nodes nodes are inserted together setting the right successor. 
	 * @param init_num_nodes the requested number of nodes that have to be initialized 
	 * @param context the context where add nodes
	 * @param space the 2D space where add nodes
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
			activeNode.setSuccessor(nextNode);
		}
		
	}

	
	/**
	 * This method is in charge to add a variable number of nodes (between min_number_joins and this.min_number_joins + join_amplitude) in the chord ring periodically, 
	 * if all the available nodes are inserted no new nodes are inserted. 
	 * To ensure that the nodes in the ring are correctly the first call is scheduled after stab_offset+stab_amplitude tick after the last node join in the initialization phase.
	 * After the first call the method is scheduled every join_interval
	 * @param context the context where add nodes
	 * @param space the 2D space where add nodes
	 */
	public void join_new_nodes(Context<Object> context, ContinuousSpace<Object> space) {
		int final_nodes_number = this.active_nodes.size() + this.min_number_joins + this.rnd.nextInt(this.join_amplitude);
		final_nodes_number  =  final_nodes_number > this.all_nodes.size() ? this.all_nodes.size() : final_nodes_number;
		HashSet<Integer> new_join_ids = new HashSet<>();
		while (this.active_nodes.size() != final_nodes_number ){
			Node rndNode =  (new ArrayList<Node>(this.all_nodes)).get(this.rnd.nextInt(this.all_nodes.size()));
			if (!this.active_nodes.contains(rndNode) ) {
				this.active_nodes.add(rndNode);
				new_join_ids.add(rndNode.getId());
				context.add(rndNode);
				space.moveTo(rndNode, rndNode.getX(), rndNode.getY());
				Node succ_node = rndNode;
				while (succ_node.equals(rndNode) && !new_join_ids.contains(rndNode.getId())){
					succ_node = (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
				}
				rndNode.join(succ_node);
			}
		}
		
	}
	
	/**
	 * This method is in charge to remove a variable number of nodes  (between min_number_leave and this.min_number_leave + leave_amplitude) in the chord ring periodically, 
	 * at least one node is left in the chord ring. 
	 * To ensure that the nodes in the ring are correctly the first call is scheduled after stab_offset+stab_amplitude tick after the last node join in the initialization phase.
	 * After the first call the method is scheduled every leave_interval
	 * @param context
	 * @param space
	 */
	public void leaving_nodes(Context<Object> context, ContinuousSpace<Object> space) {
		int exiting_nodes_number = this.min_number_leaving + this.rnd.nextInt(this.leaving_amplitude);
		exiting_nodes_number = exiting_nodes_number >= this.active_nodes.size() ? this.active_nodes.size() - 1 : exiting_nodes_number;
		for(int i = 0; i < exiting_nodes_number; i++) {
			Node rndNode =  (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
			rndNode.leave();
			context.remove(rndNode);
			this.active_nodes.remove(rndNode);
		}
	}
}

