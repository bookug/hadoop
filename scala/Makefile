cd ~/sparkapp
ls src/main/scala/*.scala
vim wc.sbt
../sbt/sbt package
spark-submit --master yarn --class "WordCount" target/scala-2.10/wordcount-project_2.10-1.0.jar > ans.txt 
mv ans.txt tmp.txt
sort tmp.txt > ans.txt 
rm tmp.txt
#NOTICE:need to compare with original hadoop output for wordcount
#the format is different: (key, value)       --     key value

