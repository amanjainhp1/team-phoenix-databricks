# Databricks notebook source
from datetime import datetime

# COMMAND ----------

class Date:
    
    def __init__(self):
        self.date = datetime.today()
        
    def getDatestamp(self):
        datestamp = self.date.strftime("%Y%m%d")
        return datestamp

    def getTimestamp(self):
        timestamp = str(int(self.date.timestamp()))
        return timestamp
