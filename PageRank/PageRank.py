def f(x):
    links=x[1][0]
    rank=x[1][1]
    n=len(links.split(","))
    result=[]
    for s in links.split(","):
        result.append((s,rank*1.0/n))
    return result

file="hdfs://172.31.222.90:9000/input/page_rank_data_small.txt"

data=sc.textFile(file)
#map to key-value pairs
link=data.map(lambda x:(x.split(":")[0], x.split(":")[1]))
#get the graph size
n=data.count()
#initial the probabilities of each node
rank=link.mapValues(lambda x:1.0/n)

for i in range(10):
    rank=link.join(rank).flatMap(f).reduceByKey(lambda x,y:x+y).mapValues(lambda x:0.15/n+0.85*x)

