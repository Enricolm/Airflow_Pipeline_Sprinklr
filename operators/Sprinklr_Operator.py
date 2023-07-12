import sys
sys.path.append('/home/enricolm/Documents/Airflow_Extracao_Dados/airflow_pipeline')

from airflow.models import BaseOperator
import pandas as pd
from pathlib import Path
from hook.extracao_hook import Extracao


class SprinklrOperator(BaseOperator):
    template_fields = ['path']
    def __init__(self,path,**kwargs):
        self.path = path
        super().__init__(**kwargs)
    

    def criando_pasta(self):
        (Path(self.path).parent).mkdir(exist_ok=True, parents=True)

    def criando_csv(self):
        data = self.data
        Data = []
        Sentimento = []
        Value = []
        for h in range (len(data)):
            
            for i in range(len(data[h]['rows'])):
                
                Data.append(data[h]['rows'][i][0])
                Sentimento.append(data[h]['rows'][i][1])
                Value.append(data[h]['rows'][i][2])
        
        dados = pd.DataFrame(data={'Data' : pd.Series(Data),'Sentimento' : pd.Series(Sentimento),'Value' : pd.Series(Value)})
        dados.to_csv(f'{self.path}', sep=';', index=False)



    def execute (self,context):
        self.criando_pasta()
        self.data = data = [] 
        for i in Extracao().run():  
            data.append(i['data'])
        self.criando_csv()



