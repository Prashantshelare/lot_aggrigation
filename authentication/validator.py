import mysql.connector
import pandas as pd
from rest_framework import authentication
from rest_framework import exceptions
from django.http import JsonResponse

def return_response(success, errorCode, error):
    return JsonResponse({
        "success": success,
        "errorCode": errorCode,
        "error": error
    }), None




class APIKeyValidator(authentication.BaseAuthentication):
    def authenticate(self, request):
        dbConn = mysql.connector.connect(
            host='cdt-ts-drk-dev-db.mysql.database.azure.com',
            port=3306,
            user='ml_user@cdt-ts-drk-dev-db',
            password='mluser@123',
            auth_plugin='mysql_native_password'
        )
        try:
            app_key = request.query_params["apiKey"]
            print('app_key =', app_key)
        except:
            return return_response(False, "LHAERR-001", "API Key is required")

        query = f""" SELECT count(*) as `found` FROM cdt_master_data.drkrishi_source where ApiKey = '{request.query_params["apiKey"]}' and Name = "Agriota";"""
        count = (pd.read_sql(query, dbConn))
        if len(count) == 0:
            return return_response(False, "LHAERR-002", "Invalid Api Key")
        return None  # authentication successful
