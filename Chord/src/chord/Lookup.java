package chord;

import repast.simphony.engine.environment.RunEnvironment;

public class Lookup {
	private Integer id;
	private Integer node_req_id;
	private Integer node_res_id;
	private Integer request_key;
	private Integer prioriCorrectNode;
	private Integer path_length;
	private Integer nodes_contacted;
	private Double starting_tick;
	private Double final_tick;
	private Boolean correctResult;
	private Boolean resultHasKey;
	private Boolean responsibleIsCrashed;
	private TopologyBuilder top;
	
	
	public Lookup(Integer id, Integer node_req_id, Integer prioriCorrectNode, Integer request_key, Double tick, TopologyBuilder top) {
		this.id = id;
		this.node_req_id = node_req_id;
		this.starting_tick = tick;
		this.prioriCorrectNode = prioriCorrectNode;
		this.request_key = request_key;
		this.top = top;
		
	}
	
	public void setResult(Node nodeRes, Integer path_length, Integer nodes_contacted) {
		this.node_res_id = nodeRes.getId();
		this.path_length = path_length;
		this.nodes_contacted = nodes_contacted;
		if(this.path_length != -1 && this.nodes_contacted != -1 ) {
			this.correctResult = nodeRes.getId() == this.prioriCorrectNode ? true : (top.firstNotCrashed(this.request_key) == nodeRes.getId());
			this.resultHasKey = nodeRes.getData().containsKey(this.request_key);
			this.responsibleIsCrashed = nodeRes.isCrashed();
		} else {
			this.correctResult = nodeRes.getId() == this.prioriCorrectNode ? true : (top.firstNotCrashed(this.request_key) == nodeRes.getId());
			this.resultHasKey = false;
			this.responsibleIsCrashed = false;
		}
		this.final_tick = RunEnvironment.getInstance().getCurrentSchedule().getTickCount();
	}
	
	public Boolean getResult() {
		return this.correctResult;
	}
	
	@Override
	public String toString() {
		String out = "";
		out += ("\nQuery id: "+this.id);
		out += ("\nRequest node: " + this.node_req_id);
		out += ("\nRequired key: " + this.request_key);
		out += ("\nRequest tick: " + this.starting_tick);
		out += ("\nPriori node : " + this.prioriCorrectNode);
		out += ("\nResponsible found: " + this.resultHasKey);
		out += ("\nResulting node: "+ this.node_res_id);
		out += ("\nNode was crashed: " + this.responsibleIsCrashed);
		out += ("\nIs the first node not crashed and initalised: " + this.correctResult);
		out += ("\nResponse tick: " + this.final_tick);
		out += ("\nPath length: " + this.path_length);
		out += ("\nNodes contacted: " + this.nodes_contacted);
		
		return out;
	}
}
