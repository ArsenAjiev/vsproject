import requests
import json
import pandas as pd
import azure.functions as func
from sqlalchemy import create_engine


def main(mytimer: func.TimerRequest) -> None:


    #1 API внешнего источника данных
    response = requests.get("https://api.thedogapi.com/v1/breeds/").text
    result = json.loads(response)
    
    # Записал в список данные из API
    data_list = []
    for data in result:
        data_list.append([data['id'], data['name']])

    # Сохранил данные в Dataframe
    df = pd.DataFrame(data=data_list, columns=['new_id', 'new_name'])



    #2 Создал подключение к БД
    engine = create_engine(
    'postgresql+psycopg2://rrodgahb:yD-fDiMNn7Z3P2KK55RjSJEj63-4pXU-@mel.db.elephantsql.com/rrodgahb') 

    # Записал данные в БД
    df.to_sql(
        name='dogs',
        con=engine,
        schema='testapi',
        if_exists='append',
        index=False
        )  
    engine.dispose()


