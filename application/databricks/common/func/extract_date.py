# Databricks notebook source
import json
import re
from datetime import datetime

# COMMAND ----------

def extract_date(str: str, regex: str, iformat: str, oformat: str) -> str:
    print(f"extract_date: {str}, {regex}, {iformat}, {oformat}")

    match = re.search(regex, str)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, iformat).strftime(oformat)
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}")
    else:
        raise ValueError(f"Date not found in: {str}")
