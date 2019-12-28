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

public class TopologyBuilder implements ContextBuilder<Object> {

	private Random rnd;
	private ArrayList<Node> all_nodes;
	private TreeSet<Node> active_nodes;
	private int min_number_joins;
	private int join_amplitude;
	private int min_number_leaving;
	private int leaving_amplitude;
	
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
		
		this.min_number_joins = params.getInteger("min_number_joins");
		this.join_amplitude = params.getInteger("join_amplitude")+1; 
		
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
		

		/*
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParams = ScheduleParameters.createRepeating(start, interval);
		schedule.schedule(scheduleParams, this, "");
		*/
		
		return context;
	}
	
	
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

	
	
	private void join_new_nodes(Context<Object> context, ContinuousSpace<Object> space) {
		int final_nodes_number = this.active_nodes.size() + this.min_number_joins + this.rnd.nextInt(this.join_amplitude);
		
		while (this.active_nodes.size() != final_nodes_number ){
			Node rndNode =  (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
			if (!this.active_nodes.contains(rndNode)) {
				this.active_nodes.add(rndNode);
				context.add(rndNode);
				space.moveTo(rndNode, rndNode.getX(), rndNode.getY());
				Node succ_node = rndNode;
				while (succ_node.equals(rndNode)){
					succ_node = (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
				}
				rndNode.join(succ_node);
			}
		}
		
	}
	
	private void exit_nodes(Context<Object> context, ContinuousSpace<Object> space) {
		int exiting_nodes_number = this.min_number_leaving + this.rnd.nextInt(this.leaving_amplitude);
		
		for(int i = 0; i < exiting_nodes_number; i++) {
			Node rndNode =  (new ArrayList<Node>(this.active_nodes)).get(this.rnd.nextInt(this.active_nodes.size()));
			rndNode.leave();
			context.remove(rndNode);
			this.active_nodes.remove(rndNode);
		}
	}
}

