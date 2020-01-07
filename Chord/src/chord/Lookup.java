package chord;

import repast.simphony.engine.environment.RunEnvironment;
import repast.simphony.util.collections.Pair;

public class Lookup {
	private Integer id;
	private Integer node_req_id;
	private Integer node_res_id;
	private Integer request_key;
	private Integer path_length;
	private Integer nodes_contacted;
	private Double starting_tick;
	private Double final_tick;
	private Pair<Boolean, Boolean> result;
	
	
	public Lookup(Integer id, Integer node_req_id, Integer request_key, Double tick) {
		this.id = id;
		this.node_req_id = node_req_id;
		this.starting_tick = tick;
		this.request_key = request_key;
	}
	
	public void setResult(Node nodeRes, Integer path_length, Integer nodes_contacted) {
		this.node_res_id = nodeRes.getId();
		this.path_length = path_length;
		this.nodes_contacted = nodes_contacted;
		if(this.path_length != -1 && this.nodes_contacted != -1 ) {
			//this.result = new Pair<Boolean, Boolean>(nodeRes.data.containsKey(this.request_key), nodeRes.isCrashed());
		}else {
			this.result = new Pair<Boolean, Boolean>(false, false);
		}
		
		this.final_tick = RunEnvironment.getInstance().getCurrentSchedule().getTickCount();
		
	}
}
