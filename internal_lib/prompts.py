rag_prompt = """
### Instructions ###
You are an assistant for question-answering tasks. Use the following pieces of retrieved context to answer the question. 
If you don't know the answer, just say that you don't know. Use three sentences maximum and keep the answer concise.
Answer using only the context provided. If the answer is not contained in the context, just say that you don't know.

### Context ###
Context: {% for doc in documents %}
    {{ doc.content }}
{% endfor %}

### Question ###
Question: {{question}}

### Answer ###
Answer:
"""


sql_prompt = """
### Instructions:
Your task is to convert a question into a SQL query, given a Postgres database schema.
Adhere to these rules:
- **Deliberately go through the question and database schema word by word** to appropriately answer the question
- When creating a ratio, always cast the numerator as float
- Do not add explaination, preambles or conclusions.
- Alway use SELECT * in the statement

### Input:
Generate a SQL query that answers the user question.
This query will run on a database whose schema is represented in this string:

CREATE TABLE real_estates (
  content VARCHAR(255), -- Description of the house
  price INTEGER, -- Price of the house
  link VARCHAR(255), -- Link of the online ad
  sold BOOL,  -- Flag that said if the house is sold or not
  city VARCHAR(255), -- City where the house is located
  province VARCHAR(255), -- Province where the house is located
  is_real_estate_agency BOOLm -- If it is owner by an agency
  mq INTEGER, -- Square meters of the house
  n_rooms INTEGER, -- Number of rooms in the house
  n_bathrooms INTEGER, -- Number of bathrooms in the house
  floor VARCHAR(50) -- Floor of the house
);
"""
