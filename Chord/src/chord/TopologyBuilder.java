package chord;

import java.util.ArrayList;
import java.util.Random;

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
	private ArrayList<Node> nodes;

	@Override
	public Context<Object> build(Context<Object> context) {
		Parameters params = RunEnvironment.getInstance().getParameters();
		
		int seed = params.getInteger("randomSeed");
		double crash_pr = params.getDouble("crash_pr");
		double recoveryInterval = params.getDouble("recovery_interval");
		double message_loss = params.getDouble("message_loss");
		
		int hash_size = params.getInteger("m");
		int num_nodes = Double.valueOf(Math.pow(2, hash_size)).intValue();
		int space_size = num_nodes*4;
		int center = space_size/2;
		int radius = (center*3)/4;
		
		context.setId("Chord");
		
		ContinuousSpaceFactory spaceFactory = ContinuousSpaceFactoryFinder.createContinuousSpaceFactory(null);
		ContinuousSpace<Object> space = spaceFactory.createContinuousSpace("space", context, new RandomCartesianAdder<Object>(),
				new repast.simphony.space.continuous.WrapAroundBorders(), space_size, space_size);

		NetworkBuilder<Object> netBuilder = new NetworkBuilder<Object>("chord_network", context, true);
		Network<Object> network = netBuilder.buildNetwork();

		this.rnd = new Random(seed);
		this.nodes = new ArrayList<>();
		for (int i = 0; i < num_nodes; i++) {
			Node node = new Node(network, this.rnd, hash_size, i, crash_pr, recoveryInterval, message_loss);
			this.nodes.add(node);
			context.add(node);
			space.moveTo(node, center+radius*Math.sin(Math.toRadians((360.0/num_nodes)*i)), center+radius*Math.cos(Math.toRadians((360.0/num_nodes)*i)));
		}
		
		for (int i = 0; i < num_nodes; i++) {
					
		}

		/*
		ISchedule schedule = RunEnvironment.getInstance().getCurrentSchedule();
		ScheduleParameters scheduleParams = ScheduleParameters.createRepeating(start, interval);
		schedule.schedule(scheduleParams, this, "");
		*/
		
		return context;
	}
}

