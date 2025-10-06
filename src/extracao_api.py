from datetime import datetime, timedelta
import os

TIME_STAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIME_STAMP_FORMAT)
end_time = datetime.now().strftime(TIME_STAMP_FORMAT)
query = 'data science'

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

url_raw = "https://api.x.com/2/tweets/search/recent?query={}&{}&{}{}{}".format(query, tweet_fields, user_fields, start_time, end_time)

bearer_token = os.environ.get("BEARER_TOKEN")
