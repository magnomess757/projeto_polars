import polars as pl
import requests 
import json
import os
import time
from dotenv import load_dotenv
from datetime import datetime
from loader import insert_data

load_dotenv()

token = os.getenv("ONFLY_TOKEN") #Acesando Variavel no .env


#Defininddo campos para requisicao 
base_url_invoices = "https://api.onfly.com.br/financial/invoice/list/invoice" 

headers_invoices = {
  'Authorization': f'Bearer {token}'
}

params_invoices = {
    "startDate": "2025-01-01",
    "endDate": datetime.now().strftime("%Y-%m-%d"),
    "type": "invoice",
    "page": 1
}

#setando lista vazia para ids de faturas
all_invoices = []

#loop para trazer todos os ids de faturas
while True:
    print(f"Baixando página {params_invoices['page']}...")
    
    response = requests.get(base_url_invoices, headers=headers_invoices, params=params_invoices)
    
    if response.status_code != 200:
        print(f"Erro {response.status_code} na página {params_invoices['page']}: {response.text}")
        break

    response_json = response.json()

    items = response_json.get('data', [])

    for fatura in items:
        id_fatura = fatura.get("id")
        all_invoices.append(id_fatura)


    meta_data = response_json.get('meta', {}).get('pagination', {})
    current_page = meta_data.get('currentPage', 1)
    total_pages = meta_data.get('total_pages', 1)


    if current_page >= total_pages:
        print("Processamento de todas as faturas concluido")
        break

    params_invoices["page"] += 1

    time.sleep(0.5)


#Definindo lista vazia para todas as faturas
all_invoices_data = []

#Logica para fazer uma requisicao para cada id
for id_fatura in all_invoices:
    print(f"request do id: {id_fatura}")

    url_details = f"https://api.onfly.com.br/financial/invoice/{id_fatura}?include=details"

    headers_details = {
        'Authorization': f'Bearer {token}'
    }

    response_details = requests.get(url_details, headers=headers_details)

    response_details_json = response_details.json()

    items_details = response_details_json.get("data", {})

    all_invoices_data.append(items_details)

    print(f'download do id {id_fatura} concluido')

    time.sleep(1)


print('Requisicao de todos ids completo')


#Verificando se "details" é um dicionario ou uma lista, se for, ele transforma details em String. Isso é para nao dar erro de estrutura na hora de transformar em DF.
for item in all_invoices_data:
    if 'details' in item and isinstance(item['details'], (dict, list)):
        item['details'] = json.dumps(item['details'])


# Criamos o DataFrame. Agora 'details' será lido como string, o que nunca falha.
df = pl.DataFrame(all_invoices_data, strict=False)


# #Definindo Schema details para decode.
details_schema = pl.Struct({
    "data": pl.List(
        pl.Struct({
            "costCenter": pl.Utf8,
            "total": pl.Int64,
            "orders": pl.List(
                pl.Struct({
                    "type": pl.Utf8,
                    "total": pl.Int64,
                    "items": pl.Struct({
                        "data": pl.List(
                            pl.Struct({
                                "id": pl.Utf8,
                                "protocol": pl.Utf8,
                                "labelStatus": pl.Utf8,
                                "reason": pl.Utf8,
                                "amount": pl.Int64,
                                "netAmount": pl.Int64,
                                "createdAt": pl.Utf8,
                                "hotelName": pl.Utf8,
                                "guests": pl.Struct({
                                    "data": pl.List(
                                        pl.Struct({
                                            "userId": pl.Int64,
                                            "name": pl.Utf8
                                        })
                                    )                                  
                                }),
                                "travellers": pl.Struct({
                                    "data": pl.List(
                                        pl.Struct({
                                            "userId": pl.Int64,
                                            "name": pl.Utf8
                                        })
                                    )                                  
                                }),
                                "description": pl.Utf8,
                                "renter": pl.Utf8 
                            })
                        )
                    })
                })
            )
        })
    )
})

#Iniciando processo de normalizacao do df
df_final = (
    df
    
    .with_columns(
        pl.col("details")
        .str.json_decode(dtype=details_schema)
        .alias("details_parsed")
    )
    .select(
        pl.col("id").alias("invoice_id"),
        pl.col("protocol").alias("invoice_protocol"),
        pl.col("companyName").alias("company_name"),
        pl.col("document").alias("company_document"),
        pl.col("dueDate").alias("due_date"),
        pl.col("description").alias("invoice_description"),
        pl.col("details").alias("details"),
        pl.col("details_parsed").struct.field("data")
    )
    .explode("data")
    
    .unnest("data")

    .rename({
        "costCenter": "cost_center",
        "total": "total_cost_center"       
    })
    
    .explode("orders")

    .unnest("orders")

    .rename({
        "type": "order_type",
        "total": "total_order"
    })

    .unnest("items")

    .explode("data")

    .unnest("data")

    .unnest("guests")

    .explode("data")

    .unnest("data")

    .rename({
        "userId": "guest_user_id",
        "name": "guest_name"
    })

    .unnest("travellers")

    .explode("data")

    .unnest("data")

    .rename({
        "userId": "traveller_user_id",
        "name": "traveller_name"
    })
)

#chamando funcao de subir dataframe
insert_data(
    df_final,
    "onfly_invoices",
    "replace"
)

