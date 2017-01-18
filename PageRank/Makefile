PageRank0:
	javac PageRank0.java
	jar -cvf PageRank0.jar PageRank0*.class 
	yarn jar PageRank0.jar PageRank0 input output
	>ans1.txt
	rm ans1.txt
	hadoop fs -get output/part-r-00000 ans1.txt
	sort -rg -k2 ans1.txt > tmp.txt
	mv tmp.txt ans1.txt
PageRank:
	javac PageRank.java
	jar -cvf PageRank.jar PageRank*.class 
	yarn jar PageRank.jar PageRank input output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-r-00000 ans.txt
	sort -rg -k2 ans.txt > tmp.txt
	mv tmp.txt ans.txt
tarball:
	tar -czvf PageRank.tar.gz PageRank.java PageRank.scala Makefile result
