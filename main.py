import os
import logging

import uvicorn

from dotenv import load_dotenv
from ast import literal_eval

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from bonsai_lib.pipelines import SubitoScraperPipeline, SubitoSearchPipeline
from bonsai_lib.components import SQLWriter
from bonsai_lib.schema import SearchQuery, GeneratorConfig


load_dotenv("config.env")


logging.basicConfig(
     level=logging.DEBUG, 
     format= '[%(asctime)s] %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
)


app = FastAPI()


@app.post("/build-index")
def build_index():

    document_store = SQLWriter(dbname="subito.db")

    # Creating preprocessing pipeline

    pipe = SubitoScraperPipeline(document_store=document_store)

    table_schema = {
        "content": "VARCHAR(255)", "price": "INTEGER", 
        "link": "VARCHAR(255)", "sold": "BOOL", "city": "VARCHAR(255)",
        "province": "VARCHAR(255)", "is_real_estate_agency": "BOOL",
        "mq": "INTEGER", "n_rooms": "INTEGER", "n_bathrooms": "INTEGER",
        "floor": "VARCHAR(50)"
        }

        
    base_url = "https://www.subito.it/annunci-sardegna/vendita/appartamenti/nuove-costruzioni/"

    urls = [base_url + "/?o={x}" for x in range(5)]
    res = pipe.run(
        {
            "fetcher": {"urls": urls},
            "document_store": {"table_name": "real_estates", "table_schema": table_schema, "create_table": True},
        }
    )

    return JSONResponse(content="Index built", status_code=200)



@app.post("/search")
def search(query: SearchQuery):

    document_store = SQLWriter(dbname="subito.db")
    # retriever = ChromaEmbeddingRetriever(document_store=document_store, top_k=15)

    # Querying pipeline

    rag_pipeline = SubitoSearchPipeline(
        generator_config=generator_config, dbname="subito.db"
    )

    response = rag_pipeline.run(
        {"prompt_builder": 
            {
                "question": query.query
            },
        }
    )

    logging.info("Results: ")
    logging.info("\n ".join(str(x) for x in response["sql_query"]["results"]))

    logging.info(f"SQL Query: *** {response['sql_query']['queries'][0]} ***")

    return JSONResponse(content=response["sql_query"]["results"], status_code=200)

if __name__ == "__main__":

    generator_config = GeneratorConfig(
        service=os.getenv("SERVICE"),
        model=os.getenv("MODEL"),
        token=os.getenv("TOKEN"),
        generation_kwargs=literal_eval(os.getenv("GENERATION_KWARGS")),
        timeout=int(os.getenv("TIMEOUT"))
    )

    uvicorn.run(app, host="0.0.0.0", port=8000)