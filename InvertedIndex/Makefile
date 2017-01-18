InvertedIndex:
	javac InvertedIndex.java
	jar -cvf InvertedIndex.jar InvertedIndex*.class 
	yarn jar InvertedIndex.jar InvertedIndex input output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-r-00000 ans.txt
	sort ans.txt > tmp.txt
	mv tmp.txt ans.txt
InvertedIndex1:
	javac InvertedIndex1.java
	jar -cvf InvertedIndex1.jar InvertedIndex1*.class 
	yarn jar InvertedIndex1.jar InvertedIndex1 input output
	>ans1.txt
	rm ans1.txt
	hadoop fs -get output/part-r-00000 ans1.txt
	sort ans1.txt > tmp.txt
	mv tmp.txt ans1.txt
InvertedIndex2:
	javac InvertedIndex2.java
	jar -cvf InvertedIndex2.jar InvertedIndex2*.class 
	yarn jar InvertedIndex2.jar InvertedIndex2 input output
	>ans2.txt
	rm ans2.txt
	hadoop fs -get output/part-r-00000 ans2.txt
	sort ans2.txt > tmp.txt
	mv tmp.txt ans2.txt
InvertedIndex3:
	javac InvertedIndex3.java
	jar -cvf InvertedIndex3.jar InvertedIndex3*.class 
	yarn jar InvertedIndex3.jar InvertedIndex3 input output
	>ans3.txt
	rm ans3.txt
	hadoop fs -get output/part-r-00000 ans3.txt
	>tmp.txt
	rm tmp.txt
	hadoop fs -get output/part-r-00001 tmp.txt
	cat tmp.txt >> ans3.txt
	sort ans3.txt > tmp.txt
	mv tmp.txt ans3.txt

