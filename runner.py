#!/usr/bin/env python3
import prestodb
import threading
import random
import argparse
import time
from os.path import join


# files referenced from prestodb repo
tpcds_100_files = [e + ".sql" for e in "q04, q14_1, q14_2, q47".replace(" ", "").split(",")]
tpcds_1000_files = [e + ".sql" for e in """q02, q03, q05, q06, q07, q09, q16, q17, q18, q19, q24_1, q24_2, q25, q27, q28, q29,
 q31, q33, q36, q38, q40, q42, q43, q46, q48, q50, q51, q52, q53, q54, q55, q56, q58, q59, q60,
  q61, q62, q63, q65, q66, q68, q70, q71, q75, q77, q79, q82, q87, q89, q91, q93, q97, q99""".replace(" ", "").replace("\n","").split(",")]



PRESTO_SERVER_HOST = "10.138.15.198"
PRESTO_SERVER_PORT = 8080
CATALOG = "hive"
PARALLELISM = 10
DEFAULT_QUERIES_PATH = "tpcds/"

thread_flags = []
thread_results = []
file_name = str(time.time())
stats_file =  open("%s.stat"%file_name, "a")
log_file = open("%s.log"%file_name, "a")


def log(message):
  print(message)
  print(message, file=log_file)

def log_stats(message):
  timestamp = str(int(time.time()))
  log("%s,%s"%(timestamp, message))
  print("%s,%s"%(timestamp, message), file=stats_file)

def fetch_memory_profile(params):
  presto_client = prestodb.dbapi.connect(
      host=params['host'],
      port=params['port'],
      user="presto_loadtest"
  )
  curr = presto_client.cursor()
  try:
    curr.execute("SELECT freebytes,maxbytes,reservedbytes,reservedrevocablebytes,object_name,node FROM jmx.current.\"presto.memory:*type=memorypool*\"")
    result = curr.fetchall()
    for e in result:
      log_stats(",".join([str(x) for x in e]))
  finally:
    curr.close()

def make_request(index, params):
  presto_client = prestodb.dbapi.connect(
      host=params['host'],
      port=params['port'],
      user="presto_benchmark_%d"%(index)
  )
  while thread_flags[index]:
    curr = presto_client.cursor()
    try:
      curr.execute(
          random.choice(params['queries'])
          .replace("${database}", params['catalog'])
          .replace("${schema}", random.choice(params['schemas']))
          )
      curr.fetchall()
      thread_results[index]+=1
    except Exception as e:
      print(e)
    finally:
      curr.close()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='load test presto cluster')
  parser.add_argument('--host', metavar='host', default=PRESTO_SERVER_HOST, help='presto server host')
  parser.add_argument('--port', metavar='port', default=PRESTO_SERVER_PORT, help='presto server port')
  parser.add_argument('--catalog', metavar='database', default=CATALOG, help='presto catalog name')
  parser.add_argument('--duration', metavar='duration', default="60", help='duration in seconds to run this test')
  parser.add_argument('--parallelisms', metavar='parallelism', default="10", help='number of queries to run in parallel, accepts comma separated list of multiple specs')
  parser.add_argument('--bucket_start_index', metavar='bucket_start_index', default="1", help='bucket starting index')
  parser.add_argument('--bucket_end_index', metavar='bucket_end_index', default="100", help='bucket ending index')
  parser.add_argument('--queries_path', metavar='queries_path', default=DEFAULT_QUERIES_PATH, help='queries to run')
  args = parser.parse_args()

  params = {
    'host': args.host,
    'port': int(args.port),
    'catalog': args.catalog,
    'queries': [f.read() for f in [open(join(args.queries_path, fs)) for fs in tpcds_1000_files]],
    'bucket_start_index': int(args.bucket_start_index),
    'bucket_end_index': int(args.bucket_end_index),
    'duration': int(args.duration),
    'schemas': ["bucket_%d_tpcds_sf100_orc" % e for e in range(int(args.bucket_start_index), int(args.bucket_end_index) + 1)]
  }

  parallelisms = [int(e) for e in args.parallelisms.split(",")]

  f = open(str(time.time())+".log", "w")
  for parallelism in parallelisms:
    log("Running %d queries in parallel" % parallelism)
    thread_flags = []
    thread_results = []
    threads = []
    start_time = time.time()
    for i in range(parallelism):
      thread_flags.append(True)
      thread_results.append(0)
      threads.append(threading.Thread(target=make_request, args=(i, params,)))
      threads[-1].start()
      time.sleep(1)

    # Keep running the queries for specified durations and log memory metrics from jmx
    waiting_time = time.time()
    while (time.time() - waiting_time) < params['duration']:
      time.sleep(60)
      fetch_memory_profile(params)

    # Tell threads to stop new queries
    for i in range(len(thread_flags)):
      thread_flags[i] = False

    # Wait for threads to stop
    for e in threads:
      e.join()

    log("Total queries run: %d in time %d\n"%(sum(thread_results), (time.time()-start_time)))
  f.close()