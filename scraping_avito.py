import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import csv
from itertools import zip_longest

URL="https://www.avito.ma/fr/maroc/voitures-%C3%A0_vendre?o"

def extract(URL):

    browser=webdriver.Chrome(ChromeDriverManager().install())
    for i in range(50):
        try:
            
                url=f"{URL}={i}"

                browser.get(url)
                prices=[ x.text for x in browser.find_elements(By.XPATH,('//span[@class="sc-1x0vz2r-0 izsKzL oan6tk-15 cdJtEx"]'))]
                names=[ x.text for x in browser.find_elements(By.XPATH,('//h3[@class="sc-1x0vz2r-0 ezkocc oan6tk-16 hlwSBL"]'))]
                link_cars=[ x.get_attribute('href') for x in browser.find_elements(By.XPATH,('//a[@class="oan6tk-1 fFOxTQ"]'))]
                time.sleep(2)
                model=[]
                for link in link_cars:
                    browser.get(link)
                    time.sleep(1)
                    features=[ x.text for x in browser.find_elements(By.XPATH,('//li[@class="sc-qmn92k-1 ldnQxr"]/span[1]'))]
                    values=[ x.text for x in browser.find_elements(By.XPATH,('//li[@class="sc-qmn92k-1 ldnQxr"]/span[2]'))]
                    model.append(dict(zip(features,values)))


                # key_words=[]
                # for x in model:
                #     key_words.extend(x.keys())




                Type=[x['Type'] if 'Type'in x.keys() else "Nan" for x in model  ]
                Number_door=[x['Nombre de portes'] if 'Nombre de portes'in x.keys() else "Nan" for x in model  ]
                kilometrage=[x['Kilométrage'] if 'Kilométrage'in x.keys() else "Nan" for x in model  ]
                first_hand=[x['Première main'] if 'Première main'in x.keys() else "Nan" for x in model  ]
                Secteur=[x['Secteur'] if 'Secteur'in x.keys() else "Nan" for x in model  ]
                marque=[x['Marque'] if 'Marque'in x.keys() else "Nan" for x in model  ]
                Etat=[x['État'] if 'État'in x.keys() else "Nan" for x in model  ]
                year_model=[x['Année-Modèle'] if 'Année-Modèle'in x.keys() else "Nan" for x in model  ]
                origin=[x['Origine'] if 'Origine'in x.keys() else "Nan" for x in model  ]

                info= [names, prices, Type, Number_door, kilometrage, first_hand, Secteur, marque,Etat,year_model,origin]
                B_data = zip_longest(*info)
                with open('cars_avito.csv', 'a') as myFile:
                    wr = csv.writer(myFile)
                    if i==0:
                        wr.writerow(["name", "price", "type", "Number_door", "kilometrage",
                                "first_hand", "Secteur", "marque","Etat","year_model","origin"])
                    wr.writerows(B_data)

        except:
            continue


    return pd.read_csv("cars_avito")    









            




