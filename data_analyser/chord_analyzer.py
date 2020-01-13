import numpy as np
import pandas as pd 
import sys,os
import matplotlib.pyplot as plt
from datetime import datetime

def wprint(s):
	print("\033[1;33;40m"+s+"\033[1;37;40m")

# Parameters for titles and calculations
possibleplots = [
	"lookupduration_time",
	"lookupduration_box",
	"errors_time",
	"errors_box",
	"timeouts_time",
	"timeouts_box",
	"nodescontacted_time",
	"nodescontacted_box",
	"lookuplength_time",
	"lookuplength_box",
	"keyspernode",
	"nodesup_time",
	"failedlookups"
]
lookup_period = 35
lookups = {
	"01":500,
	"02":500,
	"03":500,
	"04":500,
	"05":10,
	"06":25,
	"07":250,
	"08":500,
	"09":500,
	"10":500,
	"11":500,
	"12":500,
	"13":500
}
run_names = {
	"01":["Pr(c)=0.05","T(c)=60","K=1500","N=1000","skl=False"],
	"02":["Pr(c)=0.1","T(c)=60","K=1500","N=1000","skl=False"],
	"03":["Pr(c)=0.2","T(c)=60","K=1500","N=1000","skl=False"],
	"04":["Pr(c)=0.3","T(c)=60","K=1500","N=1000","skl=False"],
	"05":["Pr(c)=0.05","T(c)=60","K=30","N=1000","skl=False"],
	"06":["Pr(c)=0.05","T(c)=60","K=60","N=1000","skl=False"],
	"07":["Pr(c)=0.05","T(c)=60","K=1000","N=1000","skl=False"],
	"08":["Pr(c)=0.05","T(c)=60","K=1500","N=1000","skl=True"],
	"09":["Pr(c)=0.05","T(c)=60","K=1000","N=1000","skl=False"],
	"10":["Pr(c)=0.05","T(c)=60","K=2000","N=1000","skl=False"],
	"11":["Pr(c)=0.05","T(c)=60","K=3000","N=1000","skl=False"],
	"12":["Pr(c)=0.05","T(c)=60","K=4000","N=1000","skl=False"],
	"13":["Pr(c)=0.05","T(c)=100","K=1500","N=1000","skl=False"]
}

basefolder = sys.argv[1]
datasources = sys.argv[3].split(",")
requests = sys.argv[2].split(",")

if "all" in requests:
	for req in possibleplots:
		if req not in requests:
			requests = requests +[req]

#requests_titles = {req:0 for req in requests}

requests_titles={}
if len(sys.argv)>4:
	for req_title in sys.argv[4].split(","):
		requests_titles[req_title.split("=")[0]] = int(req_title.split("=")[1])
	print(requests_titles)
	if "all" in requests_titles.keys():
		for req in possibleplots:
			if req not in requests_titles.keys():
				requests_titles[req] = requests_titles["all"]
else:
	for req in possibleplots:
		if req not in requests_titles.keys():
			requests_titles[req] = 0		

print(requests_titles)


plotdir = "plots/"+datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
try:	
	os.makedirs(plotdir)
except OSError:
	wprint("Directory already exists, exiting.")
	sys.exit(-1)

data_node = {}
data_lookup = {}

data_node_up = {}

for dsid in datasources:
	if "loadnodes" in requests:
		if not os.path.isfile(basefolder+"/"+dsid+"_Node.csv"):
			wprint("warning: invalid file \"%s\"!"%(basefolder+"/"+dsid+"_Node.csv"))
		else:
			print("loading %s..."%(dsid+"_Node"))
			data_node[dsid+"_Node"] = pd.read_csv(basefolder+"/"+dsid+"_Node.csv")

			data_node[dsid+"_Node"]["errors"] = data_node[dsid+"_Node"].apply(lambda row:sum([int(i) for i in row["MissingWrongSuccessorsNum"][1:-1].split(",")]), axis=1)

			data_node_up[dsid+"_Node"] = data_node[dsid+"_Node"].loc[(data_node[dsid+"_Node"]["Crashed"]==False)&(data_node[dsid+"_Node"]["Subscribed"]==True)&(data_node[dsid+"_Node"]["Initialized"]==True)]

	if "loadlookups" in requests:
		if not os.path.isfile(basefolder+"/"+dsid+"_Lookup.csv"):
			wprint("warning: invalid file \"%s\"!"%(basefolder+"/"+dsid+"_Lookup.csv"))
		else:
			print("loading %s..."%(dsid+"_Lookup"))
			data_lookup[dsid+"_Lookup"] = pd.read_csv(basefolder+"/"+dsid+"_Lookup.csv")

			data_lookup[dsid+"_Lookup"]["tick"] = (data_lookup[dsid+"_Lookup"].index/lookups[dsid]).astype(int)*lookup_period


#per lookup
if "lookuplength_box" in requests:
	plt.boxplot([df.loc[df["complete"]==True]["path_length"] for df in data_lookup.values()],showfliers=False,labels=[run_names[k.split("_")[0]][requests_titles["lookuplength_box"]] for k in data_lookup.keys()])

	plt.gca().set_ylabel("Lookup steps")
	plt.gca().set_xlabel("Runs")

	plt.tight_layout()
	plt.savefig(plotdir+"/lookuplength_box.pdf")
	plt.show()

if "lookuplength_time" in requests:
	for name,df in data_lookup.items():
		df.loc[df["complete"]==True].groupby("tick",as_index = True)["path_length"].mean().plot(label=run_names[name.split("_")[0]][requests_titles["lookuplength_time"]])

	plt.gca().set_xlabel("Ticks")
	plt.gca().set_ylabel("Average lookup length (steps)")

	plt.tight_layout()
	plt.legend()
	plt.savefig(plotdir+"/lookuplength_time.pdf")
	plt.show()

if "nodescontacted_box" in requests:
	plt.boxplot([df.loc[df["complete"]==True]["nodes_contacted"] for df in data_lookup.values()],showfliers=False,labels=[run_names[k.split("_")[0]][requests_titles["nodescontacted_box"]] for k in data_lookup.keys()])

	plt.gca().set_ylabel("Nodes contacted during a lookup")
	plt.gca().set_xlabel("Runs")

	plt.tight_layout()
	plt.savefig(plotdir+"/nodescontacted_length_box.pdf")
	plt.show()

if "nodescontacted_time" in requests:
	for name,df in data_lookup.items():
		df.loc[df["complete"]==True].groupby("tick")["nodes_contacted"].mean().plot(label=run_names[name.split("_")[0]][requests_titles["nodescontacted_time"]])  

	plt.gca().set_xlabel("Ticks")
	plt.gca().set_ylabel("Average number of nodes contacted for a lookup")

	plt.tight_layout()
	plt.legend()
	plt.savefig(plotdir+"/nodescontacted_time.pdf")
	plt.show()

if "timeouts_box" in requests:
	plt.boxplot([df.loc[df["complete"]==True]["timeouts"] for df in data_lookup.values()],showfliers=True,labels=[run_names[k.split("_")[0]][requests_titles["timeouts_box"]] for k in data_lookup.keys()])

	plt.gca().set_ylabel("Timeouts during a lookup")
	plt.gca().set_xlabel("Runs")

	plt.tight_layout()
	plt.savefig(plotdir+"/nodescontacted_length_box.pdf")
	plt.show()

if "timeouts_time" in requests:
	for name,df in data_lookup.items():
		df.loc[df["complete"]==True].groupby("tick")["timeouts"].mean().plot(label=run_names[name.split("_")[0]][requests_titles["timeouts_time"]])

	plt.gca().set_xlabel("Ticks")
	plt.gca().set_ylabel("Average number of timeouts for a lookup")

	plt.tight_layout()
	plt.legend()
	plt.savefig(plotdir+"/timeouts_time.pdf")
	plt.show()

if "errors_box" in requests:
	plt.boxplot([df.groupby(["Id"],as_index=False).mean()["errors"] for df in data_node_up.values()],showfliers=True,labels=[run_names[k.split("_")[0]][requests_titles["errors_box"]] for k in data_node_up.keys()])

	plt.gca().set_ylabel("Errors in the node data structures")
	plt.gca().set_xlabel("Runs")

	plt.tight_layout()
	plt.savefig(plotdir+"/errors_box_box.pdf")
	plt.show()

if "errors_time" in requests:
	for name,df in data_node_up.items():
		df.groupby("tick")["errors"].mean().plot(label=run_names[name.split("_")[0]][requests_titles["errors_time"]])

	plt.gca().set_xlabel("Ticks")
	plt.gca().set_ylabel("Average errors in the node data structures")

	plt.tight_layout()
	plt.legend()
	plt.savefig(plotdir+"/errors_time.pdf")
	plt.show()

if "lookupduration_box" in requests:
	plt.boxplot([df.loc[df["complete"]==True]["duration"] for df in data_lookup.values()],showfliers=True,labels=[run_names[k.split("_")[0]][requests_titles["lookupduration_box"]] for k in data_lookup.keys()])

	plt.gca().set_ylabel("Timeouts during a lookup")
	plt.gca().set_xlabel("Runs")

	plt.tight_layout()
	plt.savefig(plotdir+"/nodescontacted_length_box.pdf")
	plt.show()

if "lookupduration_time" in requests:
	for name,df in data_lookup.items():
		df.loc[df["complete"]==True].groupby("tick")["duration"].mean().plot(label=run_names[name.split("_")[0]][requests_titles["lookupduration_time"]])

	plt.gca().set_xlabel("Ticks")
	plt.gca().set_ylabel("Average duration of a lookup")

	plt.tight_layout()
	plt.legend()
	plt.savefig(plotdir+"/duration_time.pdf")
	plt.show()

if "nodesup_time" in requests:
	for name,df in data_node_up.items():
		df.groupby("tick")["Id"].count().plot(label=run_names[name.split("_")[0]][requests_titles["nodesup_time"]])

	plt.gca().set_xlabel("Ticks")
	plt.gca().set_ylabel("Average errors in the node data structures")

	plt.tight_layout()
	plt.legend()
	plt.savefig(plotdir+"/errors_time.pdf")
	plt.show()

if "keyspernode" in requests:
	plt.boxplot([df.groupby(["Id"],as_index=False).mean()["DataSize"] for df in data_node_up.values()],showfliers=True,labels=[run_names[k.split("_")[0]][requests_titles["keyspernode"]] for k in data_node_up.keys()])

	plt.gca().set_ylabel("Errors in the node data structures")
	plt.gca().set_xlabel("Runs")

	plt.tight_layout()
	plt.savefig(plotdir+"/errors_box_box.pdf")
	plt.show()

if "failedlookups" in requests:
	for name,df in data_lookup.items():
		""""plt.bar(
			run_names[name.split("_")[0]][requests_titles["failedlookups"]],
			df.count(),
			log=True
		)   """  
		plt.bar(
			run_names[name.split("_")[0]][requests_titles["failedlookups"]],
			df.loc[df["complete"]==False].count(),
			#log=True
		)

	plt.gca().set_ylabel("Failed lookups")
	plt.gca().set_xlabel("Runs")

	plt.tight_layout()
	plt.savefig(plotdir+"/nodescontacted_length_box.pdf")
	plt.show()