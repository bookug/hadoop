RepartitionJoin:
	javac RepartitionJoin.java
	jar -cvf RepartitionJoin.jar RepartitionJoin*.class
	yarn jar RepartitionJoin.jar RepartitionJoin input output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-r-00000 ans.txt
	mv ans.txt repartition.txt
ImprovedRepartitionJoin:
	javac ImprovedRepartitionJoin.java
	jar -cvf ImprovedRepartitionJoin.jar ImprovedRepartitionJoin*.class
	yarn jar ImprovedRepartitionJoin.jar ImprovedRepartitionJoin input output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-r-00000 ans.txt
	mv ans.txt repartition2.txt
BroadcastJoin:
	javac BroadcastJoin.java
	jar -cvf BroadcastJoin.jar BroadcastJoin*.class
	yarn jar BroadcastJoin.jar BroadcastJoin input/table2R.dat input/table2S.dat output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-m-00000 ans.txt
	mv ans.txt broadcast.txt
	#NOTICE:teh answer of BroadcastJoin maybe not in total order
SemiJoin:
	javac SemiJoin.java
	jar -cvf SemiJoin.jar SemiJoin*.class
	yarn jar SemiJoin.jar SemiJoin keys/table2R_key.dat input output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-r-00000 ans.txt
	mv ans.txt semi.txt
NotEqualJoin:
	javac NotEqualJoin.java
	jar -cvf NotEqualJoin.jar NotEqualJoin*.class
	yarn jar NotEqualJoin.jar NotEqualJoin keys/table3R_key.dat input output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-r-00000 ans.txt
	mv ans.txt noteq.txt
data:
	javac TableGenerator.java
	java TableGenerator
	#generate table0 use the Generate.java outside, and get the keys through awk
	# the table2 is too large to run NotEqualJoin, because teh set memory of hadoop is not enough to run this
	# so table3 is produced, get the first 100 lines from table2R, and the first 1000 lines from table2S
clean:
	rm -rf *.class
tarball:
	tar -czvf HadoopJoin.tar.gz *.java Makefile DATA result

