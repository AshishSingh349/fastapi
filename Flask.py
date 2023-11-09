from flask import Flask, request, jsonify
import requests
import pandas as pd

app = Flask(__name__)

FASTAPI_URL = "http://localhost:8000"  # Replace with your FastAPI application's URL

@app.route("/get_data/<db_name>/<schema_name>/<table_name>", methods=["GET"])
def get_data(db_name, table_name,schema_name):
    # fastapi_endpoint = f"{FASTAPI_URL}/readall/{db_name}/{schema_name}/{table_name}"
    fastapi_endpoint = f"{FASTAPI_URL}/read_table_data/{db_name}/{schema_name}/{table_name}"
    response = requests.get(fastapi_endpoint)
    print(response)

    if response.status_code == 200:
        data=response.json()
        print(data)
        # data_list = [item['data'] for item in data]

        # # # Convert the list of dictionaries into a DataFrame
        # df = pd.DataFrame(data_list)
        # # df = pd.DataFrame(data)
        # print(df)
        return jsonify(response.json())
    else:
        return jsonify({"error": "Data not found"}), 404
    
@app.route("/read_col/<db_name>/<schema_name>/<table_name>", methods=["GET"])
def get_col_data(db_name, table_name,schema_name):
    # fastapi_endpoint = f"{FASTAPI_URL}/readall/{db_name}/{schema_name}/{table_name}"
    fastapi_endpoint = f"{FASTAPI_URL}/read_column_list/{db_name}/{schema_name}/{table_name}"
    response = requests.get(fastapi_endpoint)
    print(response)
    

    if response.status_code == 200:
        data=response.json()
        print(data)
        data_list = [item['column_name'] for item in data]

        print(data_list)
    
        return jsonify(response.json())
    else:
        return jsonify({"error": "Data not found"}), 404
    

@app.route("/read_pk/<db_name>/<schema_name>/<table_name>", methods=["GET"])
def get_pk_data(db_name, table_name,schema_name):
    # fastapi_endpoint = f"{FASTAPI_URL}/readall/{db_name}/{schema_name}/{table_name}"
    
    
    
  
    fastapi_endpoint_pk = f"{FASTAPI_URL}/read_table_primary_key/{db_name}/{schema_name}/{table_name}"
    
 
    response_pk = requests.get(fastapi_endpoint_pk)
    print(response_pk)
    

    if response_pk.status_code == 200:
        data=response_pk.json()
        print(data)
        data_list = [item['column_name'] for item in data]

        print(data_list)

    fastapi_endpoint = f"{FASTAPI_URL}/read_column_list/{db_name}/{schema_name}/{table_name}"
    response = requests.get(fastapi_endpoint)
    print(response)
    
    if response.status_code == 200:
        data=response.json()
        print(data)
        data_list = [item['column_name'] for item in data]

        print(data_list)
    
        return jsonify(response_pk.json())
    else:
        return jsonify({"error": "Data not found"}), 404


@app.route("/read_all_data/<db_name>/<schema_name>/<table_name>", methods=["GET"])
def get_INTEGERATED_data(db_name, table_name,schema_name):
    fastapi_endpoint = f"{FASTAPI_URL}/read_table_primary_key1/{db_name}/{schema_name}/{table_name}"   
    response=requests.get(fastapi_endpoint)
    if response.status_code == 200:
        data=response.json()
        print(data['rows'][0])
        print(data['tab_rows'])
        # data_list = [item['column_name'] for item in data['rows'][0]]

        # print(data_list)
        data_list = [item['data'] for item in data['tab_rows']]

        # Convert the list of dictionaries to a Pandas DataFrame
        df = pd.DataFrame(data_list)
        print(df)
    
        return jsonify(response.json())
    else:
        return jsonify({"error": "Data not found"}), 404
    


####################################################################################################################
##                                READ TABLE LIST IN SCHEMA
####################################################################################################################

@app.route("/get_tab_list/<db_name>/<schema_name>", methods=["GET"])
def get_tables_list(db_name, schema_name):
    fastapi_endpoint = f"{FASTAPI_URL}/table_list/{db_name}/{schema_name}"   
    response=requests.get(fastapi_endpoint)
    if response.status_code == 200:
        data=response.json()
        print(data)
        # data_list = [item['column_name'] for item in data['rows'][0]]
        return jsonify(response.json())
    else:
        return jsonify({"error": "Data not found"}), 404
    
####################################################################################################################
##                                READ ALIKE TABLE LIST IN SCHEMA
####################################################################################################################

@app.route("/get_alike_tab_list/<db_name>/<schema_name>/<table_name_partial_string>", methods=["GET"])
def get_alike_tables_list(db_name, schema_name,table_name_partial_string):
    fastapi_endpoint = f"{FASTAPI_URL}/cond_table_list/{db_name}/{schema_name}/{table_name_partial_string}"   
    response=requests.get(fastapi_endpoint)
    if response.status_code == 200:
        data=response.json()
        print(data)
        # data_list = [item['column_name'] for item in data['rows'][0]]
        return jsonify(response.json())
    else:
        return jsonify({"error": "Data not found"}), 404
    

















if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000,debug=True)