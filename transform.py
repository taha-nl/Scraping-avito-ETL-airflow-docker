import pandas as pd
import numpy as np



def fill_missing_values(data):
    #replacing Nan(string) with np.nan 
    data=data.replace({'Nan':np.nan})

    ### filling missing values with mode for categorical variables and with mean for continuous variables
    data=data.fillna({"Etat":data["Etat"].mode()[0],
                                "price":data['price'].mean(),
                                        "Number_door":data['Number_door'].mode()[0],
                                                "first_hand":data['first_hand'].mode()[0],
                                                            "origin":data['first_hand'].mode()[0] })

    return data                                                




def drop_columns(data):
    ##" removing unimportant columns
    data=data.drop(["name","Secteur","type"],axis=1)
    return data


def kilometrage(data):

    data["minimum_distance"]=data["kilometrage"].str.split("-").apply(lambda x:x[0].replace(" ",""))
    data["maximum_distance"]=data["kilometrage"].str.split("-").apply(lambda x:x[1].replace(" ",""))
    data["minimum_distance"]=data["minimum_distance"].astype(int)
    data["maximum_distance"]=data["maximum_distance"].astype(int)
   


    return data

def transform(data):
    data=fill_missing_values(data)
    data=drop_columns(data)
    data=kilometrage(data)

    return data



    








