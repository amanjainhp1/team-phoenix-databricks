# Databricks notebook source
from datetime import datetime

# COMMAND ----------

class Date:
    
    def __init__(self):
        self.date = datetime.today()
        
    def getDatestamp(self, output_string_format: str = "%Y%m%d"):
        datestamp = self.date.strftime(output_string_format)
        return datestamp

    def getTimestamp(self):
        timestamp = str(int(self.date.timestamp()))
        return timestamp
