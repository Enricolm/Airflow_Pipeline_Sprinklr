from airflow.providers.http.hooks.http import HttpHook
import requests
import json


class Extracao (HttpHook):
    def __init__(self,conn_id = None):
        self.conn_id = conn_id or 'sprinklr_connection'
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):

        url= f"{self.base_url}/prod2/api/v2/reports/query"
        return url
    
    def conecao_endpoint(self, url,session,i):
        with open('/home/enricolm/Documents/Airflow_Extracao_Dados/airflow_pipeline/hook/payload.json', 'r+') as j:
            payload = json.load(j)

        payload['page'] = i

        with open('/home/enricolm/Documents/Airflow_Extracao_Dados/airflow_pipeline/hook/payload.json', 'w') as arquivo:
                json.dump(payload,arquivo,indent=4)
            
        request = requests.Request('POST',url=url, json=payload)
        prep = session.prepare_request(request)
        return self.run_and_check(session, prep, {})

    def paginando(self,url,session):
        i = 0
        lista_json_paginados = []
        while True:
            print(i)
            request = self.conecao_endpoint(url=url,session=session,i=i)
            response = request.json()
            print(len(response['data']['rows']))
            lista_json_paginados.append(response)
            i = i + 1
            if len(response['data']['rows']) < 60:
                break
        return lista_json_paginados

    def run(self):
        session = self.get_conn()
        url = self.create_url()

        return self.paginando(url,session)


