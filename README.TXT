###############
TASK 1 
###############
Compilation Steps:
1. hdfs dfs -put ~/inputTweets.txt/ ~/input
2. hadoop jar wc.jar WordCount ~/inputTweets.txt ~/output1
3. hdfs dfs -get ~/output1

###############
TASK 2 
###############
Compilation Steps:

1. hdfs dfs -put ~/inputTweets.txt/ ~/input
2. hadoop jar wcPairs.jar ~/input/inputTweets.txt ~/pairsOutput
3. hadoop jar wcStripes.jar ~/input/inputTweets.txt ~/stripesOutput
4. hdfs dfs -get ~/pairsOutput
5. hdfs dfs -get ~/stripesOutput


###############
ACTIVITY 1 
###############
1. hdfs dfs -put ~/texts/ ~/input
2. hadoop jar WordCountActivity1.jar WordCountActivity1 ~/input/texts ~/outputFolder1 ~/la.lexicon.csv


###############
ACTIVITY 2 
###############
1. hdfs dfs -put ~/latin/ ~/input
2. hadoop jar wc.jar WordCooccurrence ~/input/latin ~/outputFolder1 ~/la.lexicon.csv
