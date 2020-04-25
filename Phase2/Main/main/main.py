from flask import Flask, send_file
import seaborn as sns
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas
from io import BytesIO
i
app = Flask(__name__)


@app.route("/query/<id>", methods=['GET'])
def hello(id):
    if id == "1":
        query1 = spark.sql(
            "select screen_name as Username, max(followers_count) as No_of_Followers from WWE_Users group by screen_name "
            "order by No_of_followers desc limit 5")
        pd1 = query1.toPandas()
        pd1.plot.area(x="Username",y="No_of_Followers")
        plt.title("usernames with most number of followers")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "2":
        query2 = spark.sql("select substring(user.created_at,1,3) as Day, count(user.id) as Tweet_count from "
                            "WWE_Tweets where substr(user.created_at,1,3) is not null group by Day")
        pd2 = query2.toPandas()
        pd2.plot.pie(y="Tweet_count", labels=pd2.Day.values.tolist(), autopct='%.2f')
        plt.title("Number of tweets based on day")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "3":
        query3 = spark.sql("Select user.screen_name,count(*) as tweet_count from WWE_Tweets group by user.screen_name "
                           "order by tweet_count desc limit 5")
        pd3 = query3.toPandas()
        pd3final = pd3.dropna()
        #sns.catplot(y="screen_name",x="tweet_count",data=pd3final)
        sns.catplot(x='screen_name', y='tweet_count', data=pd3final,height=6,aspect=2).set(title='Top user with most tweets')
        #plt.title("Top user with most tweets")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "4":
        query4 = spark.sql("select user.screen_name,text,retweeted_status.retweet_count from WWE_Tweets "
                            "order by retweeted_status.retweet_count DESC limit 10")
        pd4 = query4.toPandas()
        pd4.plot.pie(y="retweet_count",labels=pd4.screen_name.values.tolist(),autopct='%.2f')
        plt.title("Users with more no of retweets for his tweet")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "5":
        query5 = spark.sql(
            "select count(*) as Count,q.text from (select case when text like '%John Cena%' then 'John Cena' when text "
            "like '%Rock%' then 'Rock'when text like '%Roman Reigns%' then 'Roman Reigns' when text like '%Kane%' then "
            "'Kane' when text like '%Sasha Banks%' then 'Sasha Banks' when text like '%Undertaker%' then"
            " 'Undertaker' else 'Different_player' end as text from WWE_Tweets) q group by q.text")
        pd5 = query5.toPandas()
        pd5.plot(x="text",y='Count',figsize=(10,5))
        plt.title("No of tweets for a particular player")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "6":
        query6 = spark.sql(
            "select substring(user.created_at,27,4) as year, count(*) as Count from WWE_Tweets where user.created_at is"
            " not null group by substring(user.created_at,27,4) order by count(*) desc")
        pd6 = query6.toPandas()
        pd6.plot.bar(x="year",y="Count")
        plt.title("No of tweets created by users per year")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "7":
        query7 = spark.sql(
            "select place.country, count(*) as count from WWE_Tweets where place.country is not null group by "
            "place.country order by count desc limit 10")
        pd7 = query7.toPandas()
        pd7.plot(x="country",y="count")
        plt.title("Top countries where tweets came from")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "8":
        day_data = spark.sql("SELECT substring(user.created_at,1,3) as day from WWE_Tweets where text is not null")
        day_data.createOrReplaceTempView("day_data")
        days_final = spark.sql(
            """ SELECT Case
              when day LIKE '%Mon%' then 'WEEKDAY'
              when day LIKE '%Tue%' then 'WEEKDAY'
              when day LIKE '%Wed%' then 'WEEKDAY'
              when day LIKE '%Thu%' then 'WEEKDAY'
              when day LIKE '%Fri%' then 'WEEKDAY'
              when day LIKE '%Sat%' then 'WEEKEND'
              when day LIKE '%Sun%' then 'WEEKEND'
               else
               null
               end as day1 from day_data where day is not null""")
        days_final.createOrReplaceTempView("days_final")
        query8 = spark.sql("SELECT day1 as Day,Count(*) as Day_Count from days_final where day1 is not null group by "
                           "day1 order by count(*) desc")
        pd8 = query8.toPandas()
        sns.catplot(x="Day",y="Day_Count",data=pd8,kind="point").set(title="Tweets posted on weekend and weekday")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "9":
        query9 = spark.sql("select lang, count(1) Tweets from WWE_Tweets group by lang order by Tweets desc limit 10")
        pd9 = query9.toPandas()
        sns.catplot(x='lang',y='Tweets',data=pd9,height=5,aspect=2).set(title = 'Tweets in diff Languages')
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')
    if id == "10":
        query10 = spark.sql("select substring(user.created_at,5,3) as month,count(user.id) as No_of_Tweets from "
                            "WWE_Tweets where substr(user.created_at,5,3) is not null group by month")
        pd10 = query10.toPandas()
        pd10.plot.pie(y="No_of_Tweets",labels=pd10.month.values.tolist(),autopct='%.2f')
        plt.title("Number of Tweets by users based on month")
        img = BytesIO()
        plt.savefig(img)
        img.seek(0)
        return send_file(img, mimetype='image/png')



if __name__ == "__main__":
    spark = SparkSession.builder.appName("Phase 2 querying and plotting").getOrCreate()
    sc = spark.sparkContext
    df = spark.read.json(r"C:\Users\sarik\OneDrive\Desktop\wwee.json")
    df.createOrReplaceTempView("WWE_Tweets")
    user = df.select('user.screen_name', 'user.followers_count', 'id').distinct()
    # user.show()
    user.createOrReplaceTempView('WWE_Users')
    app.run(debug=True)
