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
Your task is to convert a question into a SQL query, given a SQL database schema.
Adhere to these rules:
- **Deliberately go through the question and database schema word by word** to appropriately answer the question
- When creating a ratio, always cast the numerator as float
- Limit to 5 results at most

### Input:
Generate a SQL query that answers the question `{{question}}`.
This query will run on a database whose schema is represented in this string:

TABLE real_estates (
  content VARCHAR(255), -- Description of the house
  price INTEGER, -- Price of the house
  link VARCHAR(255), -- Link of the ad
  sold BOOL,  -- If the house is sold or not
  city VARCHAR(255), -- City of the house
  province VARCHAR(255), -- Province of the house
  is_real_estate_agency BOOLm -- If it is owner by an agency
  mq INTEGER, -- Square meters of the house
  n_rooms INTEGER, -- Number of rooms in the house
  n_bathrooms INTEGER, -- Number of bathrooms in the house
  floor VARCHAR(50) -- Floor of the house
);

### Response:
Based on your instructions, here is the SQL query I have generated to answer the question `{{question}}`:
```sql
"""


template_prompt = """
{% if query_to_validate %}
    Here was the text you were provided:
    {{ question }}
    Here is the query you previously generate: 
    {{ query_to_validate[0] }}
    Are the query correct? 
    Things to check for:
    - Table name should be {{ table_name }}
    - Column names should be {{ column_names }}
    - The reply should be in this format: [SQL] QUERY [\SQL]

    If the query is correct, say 'DONE' and return the SQL query
    If not, simply return the best SQL query you can come up with.
    
    Based on your instructions, here is the SQL query:

{% else %} """ + sql_prompt + """ {% endif %}
"""