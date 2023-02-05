import pandas as pd 

def extract():
    """
       function for extracting 
          data from a csv file
    """
    avito_df=pd.read_csv("avito.csv")
    
    return avito_df