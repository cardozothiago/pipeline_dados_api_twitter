from airflow.providers.http.hooks.http import HttpHook

class ApiHook(HttpHook):
    def __init__(self, end_time, start_time, query, conn_id=None):
        super().__init__(http_conn_id=conn_id or "labdados_default")
        self.start_time = start_time
        self.end_time = end_time
        self.query = query

    def create_url(self):
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        return f"{self.base_url}/2/tweets/search/recent?query={self.query}&{tweet_fields}&{user_fields}&start_time={self.start_time}&end_time={self.end_time}"

    def connect_to_api(self, url, session):
        self.log.info(f"Fetching URL: {url}")
        response = session.get(url)
        response.raise_for_status()
        return response

    def paginate(self, url_raw, session):
        response_list = []
        response = self.connect_to_api(url_raw, session)
        json_response = response.json()
        response_list.append(json_response)
        count = 1

        while 'next_token' in json_response.get('meta', {}) and count < 80:
            next_token = json_response['meta']['next_token']
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_api(url, session)
            json_response = response.json()
            response_list.append(json_response)
            count += 1
        
        return response_list  # lista de dicts

    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        return self.paginate(url_raw, session)
