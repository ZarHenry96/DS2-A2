package chord;

import repast.simphony.engine.environment.RunEnvironment;
import repast.simphony.util.collections.Pair;

public class Lookup {
	private Integer id;
	private Node node_req;
	private Integer node_res_id;
	private Integer request_key;
	private Integer path_length;
	private Integer nodes_contacted;
	private Double starting_tick;
	private Double final_tick;
	private Pair<Boolean, Boolean> result;
	
	
	public Lookup(Integer id, Node node_req, Integer request_key, Double tick) {
		this.id = id;
		this.node_req = node_req;
		this.starting_tick = tick;
		this.request_key = request_key;
	}
	
	public void setResult(Node nodeRes, Integer path_length, Integer nodes_contacted) {
		this.node_res_id = nodeRes.getId();
		this.path_length = path_length;
		this.nodes_contacted = nodes_contacted;
		if(this.path_length != -1 && this.nodes_contacted != -1 ) {
			this.result = new Pair<Boolean, Boolean>(nodeRes.getData().containsKey(this.request_key), nodeRes.isCrashed());
		} else {
			this.result = new Pair<Boolean, Boolean>(false, false);
		}
		
		/*
		if(!this.result.getFirst()) {
			System.out.println("QUerrrrrrrrrrrrrrrrrrryy");
			System.out.println(this.starting_tick);
			System.out.println(this.request_key);
			System.out.println(this.node_req.getId());
			node_req.debug();
			System.out.println(this.path_length);
			System.out.println(this.nodes_contacted);
			RunEnvironment.getInstance().pauseRun();
		}
		*/
		this.final_tick = RunEnvironment.getInstance().getCurrentSchedule().getTickCount();
		
	}
	
	public Boolean getResult() {
		return this.result.getFirst();
	};
}
