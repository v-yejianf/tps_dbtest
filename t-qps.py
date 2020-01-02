import multiprocessing
import sys, os, time, random 
import datetime
from pygresql import pg

def pg_connect(start,rt_list,keeps):
    gpdb = pg.DB(host='mas01',dbname='csgbi',user='gpadmin6',port=6666)
    end = time.time()
    runtime = end - start
    while runtime <= keeps:
      try:
        mystart = time.time()
        ran = random.randrange(1,1000000000)
        query = """
            insert into test values(%s,'test1','test2','test3')
            """%(ran)
        gpdb.query(query)
        myend = time.time()
        rt = myend - mystart
      except Exception as e:
        print e
        rt = 0
      end = time.time()
      runtime = end - start
      rt_list.append(rt)
    gpdb.close()

def pg_connect_current(start,rt_list,keeps):
    end = time.time()
    runtime = end - start
    while runtime <= keeps:
      try:
        mystart = time.time()
        ran = random.randrange(1,1000000000)
        query = """
                insert into test values(%s,'test1','test2','test3')
                """%(ran)
        gpdb = pg.DB(host='mas01',dbname='csgbi',user='gpadmin6',port=6666)
        gpdb.query(query)
        gpdb.close()
        myend = time.time()
        rt = myend - mystart
      except Exception as e:
        print e
        rt = 0
      end = time.time()
      runtime = end - start
      rt_list.append(rt)

def create_process(threads,keeps):
    global rt_list
    start = time.time()
    rt_list = multiprocessing.Manager().list()
    p = multiprocessing.Pool(threads)
    for i in range(threads):
        p.apply_async(pg_connect, args=(start,rt_list,keeps))
    p.close()
    p.join()

if __name__=='__main__':
    print "pid:%s"%(os.getpid())
    threads = int(sys.argv[1])
    keeps = int(sys.argv[2])
    rt_list=[]
    
    mystart = time.time()
    create_process(threads,keeps)
    myend = time.time()
    totaltime = myend - mystart
    
#check result...
    runcnt = 0
    errcnt = 0
    rt_sum = 0
    rt_min = 100
    rt_max = 0
    for i in rt_list:
      val = float(i)
      rt_sum += val
      runcnt += 1
      if rt_min >= val:
        rt_min = val
      if rt_max <= val:
        rt_max = val
      if float(i) == float(0):
        errcnt += 1
    rt_avg = rt_sum / runcnt
    tps = (runcnt - errcnt) / keeps
    print "totaltime:%.4f"%(totaltime)
    print "runcnt:%s errcnt:%s rt_avg:%.4f(s) rt_min:%.4f(s) rt_max:%.4f(s)"%(runcnt,errcnt,rt_avg,rt_min,rt_max)
    print "TPS:%.0f"%(tps)


