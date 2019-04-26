import time, sys, subprocess, json, requests, threading, itertools, time, datetime, traceback

# Given a compute name and port, starts port forwarding LLAP executors starting from given port number and prints CPU usage every 5 seconds
# Usage: python3 profiling.py <compute-name> <portnum>
# Example: python3 profiling.py compute-1555097648-j2rv 25002

def run_check(threadid, delay, port, task):
	next_time = time.time() + delay
	req = URL.replace("PORT", str(port))
	while True:
	    time.sleep(max(0, next_time - time.time()))
	    try:
	      task(threadid, req)
	    except Exception:
	      traceback.print_exc()
    # skip tasks if we are behind schedule:
	    next_time += (time.time() - next_time) // delay * delay + delay

def getmetrics(threadid, req):
	r = requests.get(req)
	data = r.json()
#	print (','.join(["query-executor-"+str(threadid), datetime.datetime.now().strftime("%H:%M:%S"), str(data['beans'][7]), str(data['beans'][11]), str(data['beans'][58]), str(data['beans'][17]),str(data['beans'][67])]))
	jmx_list=list(data.values())
	metrics = []
	i = 0
	for item in itertools.chain.from_iterable(jmx_list):
		if 'CurrentThreadCpuTime' in item:
			metrics.append("CurrentThreadCpuTime")
			metrics.append(str(item["CurrentThreadCpuTime"]))
		elif 'MemHeapUsedM' in item:
			metrics.append("MemHeapUsedM")
			metrics.append(str(item["MemHeapUsedM"]))
		elif 'SystemCpuLoad' in item:
			metrics.append("SystemCpuLoad")
			metrics.append(str(item["SystemCpuLoad"]))
			metrics.append("ProcessCpuLoad")
			metrics.append(str(item["ProcessCpuLoad"]))
			metrics.append("ProcessCpuTime")
			metrics.append(str(item["ProcessCpuTime"]))
#		elif 'CacheHitRatio' in item:
#			metrics.append("CacheHitRatio")
#			metrics.append(str(item["CacheHitRatio"]))
#		elif 'tag.Context' in item and item["tag.Context"] == "s3aFileSystem":
#			s3metrics = {key:val for key,val in filter(lambda t: isinstance(t[1], int) and t[1] > 0, item.items())}
#			if s3metrics:
#				metrics.append("S3 metrics")
#				metrics.append(str(s3metrics))
#				s3metrics = []
		else:
			continue
			
	print (','.join(["query-executor-"+str(threadid), datetime.datetime.now().strftime("%H:%M:%S"), str(metrics)]))


COMPUTE_NAME=sys.argv[1]
PORT=sys.argv[2]
URL = "http://localhost:PORT/jmx"

NUMCMD = "kubectl get pods --namespace COMPUTENAME | grep executor | wc -l"
numexectors = int(subprocess.check_output(NUMCMD.replace("COMPUTENAME", COMPUTE_NAME), shell=True))

FORWARDIND_PORT_CMD="kubectl port-forward -n COMPUTENAME query-executor-NUM HOSTPORT:25002 --pod-running-timeout=60m"

for i in range(numexectors):
	port = str(int(PORT) + i)
	num = str(i)
	cmd=FORWARDIND_PORT_CMD.replace("COMPUTENAME",COMPUTE_NAME).replace("NUM", num).replace("HOSTPORT", port)
	num = str(i)
	cmd=FORWARDIND_PORT_CMD.replace("COMPUTENAME",COMPUTE_NAME).replace("NUM", num).replace("HOSTPORT", port)
	p = subprocess.Popen(cmd, shell=True,stdin=None, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

	print ("Forwardport pid =" + str(p.pid))

print ("Started profiling")

for i in range(numexectors):
	threading.Thread(target=lambda: run_check(i, 5, int(PORT)+i, getmetrics)).start()

