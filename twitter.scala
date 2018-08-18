val tweets=sqlContext.jsonFile("/home/bhanupal/workspace/spark/twitter/data");
tweets.registerTempTable("tweet");
val extracted_tweets=sql("select id,text from tweet").collect
val AFINN = sc.textFile("/home/bhanupal/workspace/spark/twitter/AFINN.txt").map(x=>x.split("\t")).map()map(x=>(x(0).toString,x(1).toInt));
val tweetsSenti=extracted_tweets.map(tweetText=>{
	val tweetWordSentiment=tweetText(1).toString.split(" ").map(word=>{
		var senti: Int = 0;
		if(AFINN.lookup(word.toLowerCase()).length >0){
			senti=AFINN.lookup(word.toLowerCase())(0)
		};
		senti;
		});
	val tweetSentiment = tweetWordsSentiment.sum
	(tweetSentiment,tweetText.toString)
	})
val tweetsSentiRDD: org.apache.spark.rdd.RDD[(Int, String)] = sc.parallelize(tweetsSenti.toList).sortBy(x => x._1, false);