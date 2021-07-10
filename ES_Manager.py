import os
from elasticsearch import Elasticsearch, RequestError
from config import ElasticSearchDetails

ESDetails = ElasticSearchDetails()

class ESM():
    def __init__(self, ElasticEndpoint=None):
        self.ElasticEndpoint = ElasticEndpoint
        self.username = ESDetails.username
        self.password = ESDetails.password
        self.host = ESDetails.host
        self.port = ESDetails.port
        self.error = None

    def Check_ES_Conn(self):
        try:
            self.es = Elasticsearch(['https://{user}:{password}@{host}:{port}'.format(
                user=self.username, password=self.password, host=self.host, port=self.port)])
        except RequestError as e:
            self.error = e
        return self

    def Search_Result(self, query, index):
        self.response = self.es.search(index=index, body=query)
        return self.response
