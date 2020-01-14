package chord;

import repast.simphony.engine.environment.RunEnvironment;

/**
 * This class provides a useful structure for keeping track of the information related to a lookup operation
 */
public class Lookup {
	private Integer id;
	private Integer request_key;
	private Integer node_req_id;
	private Double starting_tick;
	private Integer prioriCorrectNode;
	private Boolean completed;
	private Integer node_res_id;
	private Integer path_length;
	private Integer num_timeouts;
	private Integer nodes_contacted;
	private Double final_tick;
	private Boolean correctResult;
	private Boolean resultHasKey;
	private Boolean responsibleIsCrashed;
	private TopologyBuilder top;
	
	/**
	 * Public constructor
	 * @param id id of the lookup operation
	 * @param request_key key to look for (target key)
	 * @param node_req_id id of the lookup initiator
	 * @param tick starting simulation tick
	 * @param prioriCorrectNode id of the correct result
	 * @param top reference to the TopologyBuilder
	 */
	public Lookup(Integer id,  Integer request_key, Integer node_req_id, Double tick, Integer prioriCorrectNode, TopologyBuilder top) {
		this.id = id;
		this.request_key = request_key;
		this.node_req_id = node_req_id;
		this.starting_tick = tick;
		this.prioriCorrectNode = prioriCorrectNode;
		this.top = top;
		this.completed = false;
	}
	
	/**
	 * Sets the result information
	 * @param nodeRes reference to the responsible for the target key
	 * @param path_length length of the path between the lookup initiator and the responsible
	 * @param num_timeouts number of timeouts encountered during the lookup
	 * @param nodes_contacted number of nodes contacted
	 * @param delay_response random delay for a simulated response
	 */
	public void setResult(Node nodeRes, Integer path_length, Integer num_timeouts, Integer nodes_contacted, Double delay_response) {
		this.completed = true;
		this.node_res_id = nodeRes.getId();	
		this.path_length = path_length;
		this.num_timeouts = num_timeouts;
		this.nodes_contacted = nodes_contacted;
		this.final_tick = RunEnvironment.getInstance().getCurrentSchedule().getTickCount() + delay_response;
		if(this.path_length != -1 && this.nodes_contacted != -1 ) {
			this.correctResult = nodeRes.getId() == this.prioriCorrectNode ? true : (top.firstNotCrashed(this.request_key) == nodeRes.getId());
			this.resultHasKey = nodeRes.getData().containsKey(this.request_key);
			this.responsibleIsCrashed = nodeRes.isCrashed();
		} else {
			this.correctResult = false;
			this.resultHasKey = false;
			this.responsibleIsCrashed = false;
		}
	}
	
	/**
	 * Returns if the responsible has been found or if the node retrieved has the target key but is crashed
	 * @return true if the responsible has been found or if the node found has the target key but is crashed, false otherwise
	 */
	public Boolean getResult() {
		return this.correctResult || (this.resultHasKey && this.responsibleIsCrashed);
	}
	
	/**
	 * Returns if the lookup has been completed
	 * @return true if it has been completed, false otherwise
	 */
	public Boolean isComplete() {
		return this.completed;
	}
	
	/**
	 * Returns a string in CSV format containing the relevant information for the analysis 
	 * @return a string in CSV format containing the relevant information for the analysis
	 */
	public String toCSV() {
		String csv_entry = "";
		csv_entry += this.completed +",";
		if(this.completed) {
			csv_entry += (this.final_tick-this.starting_tick) + ",";
			csv_entry += this.correctResult + ",";	
			csv_entry += this.resultHasKey + ",";	
			csv_entry += this.responsibleIsCrashed + ",";	
			csv_entry += this.path_length + ",";	
			csv_entry += this.num_timeouts + ",";
			csv_entry += this.nodes_contacted + "\n";
		} else {
			csv_entry += "-1,";
			csv_entry += false + ",";	
			csv_entry += false + ",";	
			csv_entry += false + ",";	
			csv_entry += "-1,";	
			csv_entry += "-1,";
			csv_entry += "-1\n";
		}
		return csv_entry;
	}
	
	@Override
	public String toString() {
		String out = "";
		out += ("\nQuery id: "+this.id);
		out += ("\nRequired key: " + this.request_key);
		out += ("\nRequest node: " + this.node_req_id);
		out += ("\nRequest tick: " + this.starting_tick);
		out += ("\nPriori node : " + this.prioriCorrectNode);
		if(this.completed) {
			out += ("\nResult node: "+ this.node_res_id);
			out += ("\nPath length: " + this.path_length);
			out += ("\nNum timeouts: " + this.num_timeouts);
			out += ("\nNodes contacted: " + this.nodes_contacted);
			out += ("\nResponse tick: " + this.final_tick);
			out += ("\nResponsible found: " + this.correctResult);
			out += ("\nKey is there: " + this.resultHasKey);
			out += ("\nResult is crashed: " + this.responsibleIsCrashed);
		}
		
		return out;
	}
}
