import requests
import json
import datetime
import pprint

import os
from dotenv import load_dotenv

class StocksService:
    def __init__(self, 
                 api_key : str = None, 
                 base_url : str  = "https://www.alphavantage.co/query",
                ):
        load_dotenv()
        self.api_key : str = api_key if api_key is not None else os.getenv("API_KEY")
        self.base_url : str = base_url

    def get(self, params, filename : str = ""):
        print("Before get, with params:")
        pprint.pprint(params)
        response = requests.get(
            url = self.base_url,
            params=params,
        )
        print("After get, response:")
        pprint.pprint(response)

        if response.status_code == 200:
            print("Code 200")
            if filename != None and len(filename) != 0:
                print("Writing to file")
                with open(f"{filename}.json", "w") as response_file:
                    response_file.write(response.content.decode("utf-8"))
            print("Returning json: ")
            pprint.pprint(response.json())
            return response.json()

        print(response.status_code)
        return None
    
    def read_file(self, filename):
        print(f"Reading from file {filename}")
        try:
            with open(f"{filename}.json", "r") as file:
                print("File found :)")
                return json.loads(file.read())
        except:
            print("Error opening file")
            return None
        return None
    
    def get_time_series_daily(self, symbol : str, read_from_file : bool = False):
        filename = f"time_series_daily_{symbol}"

        if read_from_file:
            return self.read_file(filename)    
        
        if symbol is None or len(symbol) == 0:
            return None
        
        function = "TIME_SERIES_DAILY"
        params = {
            "function" : function,
            "symbol" : symbol,
            "apikey" : self.api_key,
        }

        return self.get(params = params, filename = filename)
        
    
    def is_valid_date(string_date : str):
        try:
            datetime.datetime.strptime(string_date, "%Y-%m")
            return True
        except:
            return False
    
    def get_simple_moving_average(self,
                                  symbol : str, 
                                  time_period : int, 
                                  series_type : str = "close",
                                  interval : str = "daily", 
                                  month: str = "",
                                  read_from_file : bool = False
                                ):
        filename = f"simple_moving_average_{symbol}"
        if read_from_file:
            print("Reading from file")
            return self.read_file(filename=filename)
    
        function = "SMA"
        if symbol is None or len(symbol) == 0:
            return None
        
        if interval not in ["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"]:
            interval = "daily"

        if time_period <= 0:
            return None
        
        if series_type not in ["close", "open", "high", "low"]:
            series_type = "close"

        if not(StocksService.is_valid_date(month)) or len(month) == 0:
            month = None

        params = {
            "function" : function,
            "symbol" : symbol,
            "interval" : interval,
            "month" : month,
            "time_period" : time_period,
            "series_type" : series_type,
            "apikey" : self.api_key,
        }

        params = {k: v for k, v in params.items() if v is not None}

        return self.get(params = params, filename=filename)
    

    def get_inflation(self, read_from_file : bool = False):
        filename = "inflation"
        if read_from_file:
            return self.read_file(filename=filename)
    
        function = "INFLATION"

        params = {
            "function" : function,
            "apikey" : self.api_key,
        }

        print("Ready to get")
        return self.get(params = params, filename=filename)
    

    def get_federal_funds_rate(self, interval : str = "monthly", read_from_file : bool = False):
        filename = "federal_funds_rate"
        if read_from_file:
            return self.read_file(filename=filename)
    
        function = "FEDERAL_FUNDS_RATE"

        if interval not in ["daily", "weekly", "monthly"]:
            interval = "monthly"

        params = {
            "function" : function,
            "interval" : interval,
            "apikey" : self.api_key,
        }

        return self.get(params=params, filename=filename)

    def get_company_overview(self, symbol : str, read_from_file : bool = False):
        filename = f"company_overview_{symbol}"

        if read_from_file:
            return self.read_file(filename=filename)
        
        function = "OVERVIEW"
        if symbol is None or len(symbol) == 0:
            return None
        
        params = {
            "function" : function,
            "symbol" : symbol,
            "apikey" : self.api_key,
        }

        return self.get(params, filename)