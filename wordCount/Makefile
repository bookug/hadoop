WordCount:
	javac WordCount.java
	jar -cvf WordCount.jar WordCount*.class 
	yarn jar WordCount.jar WordCount input/WordCountSmall output
	>ans.txt
	rm ans.txt
	hadoop fs -get output/part-r-00000 ans.txt
	sort ans.txt > tmp.txt
	mv tmp.txt ans.txt
WordCount1:
	javac WordCount1.java
	jar -cvf WordCount1.jar WordCount1*.class 
	yarn jar WordCount1.jar WordCount1 input/WordCountSmall output
	>ans1.txt
	rm ans1.txt
	hadoop fs -get output/part-r-00000 ans1.txt
	sort ans1.txt > tmp.txt
	mv tmp.txt ans1.txt
WordCount2:
	javac WordCount2.java
	jar -cvf WordCount2.jar WordCount2*.class 
	yarn jar WordCount2.jar WordCount2 input/WordCountSmall output
	>ans2.txt
	rm ans2.txt
	hadoop fs -get output/part-r-00000 ans2.txt
	sort ans2.txt > tmp.txt
	mv tmp.txt ans2.txt
WordCount3:
	javac WordCount3.java
	jar -cvf WordCount3.jar WordCount3*.class 
	yarn jar WordCount3.jar WordCount3 input/WordCountSmall output
	>ans3.txt
	rm ans3.txt
	hadoop fs -get output/part-r-00000 ans3.txt
	>tmp.txt
	rm tmp.txt
	hadoop fs -get output/part-r-00001 tmp.txt
	cat tmp.txt >> ans3.txt
	sort ans3.txt > tmp.txt
	mv tmp.txt ans3.txt

