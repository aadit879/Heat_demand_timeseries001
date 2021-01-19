# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 14:39:02 2021

@author: Aadit
"""
from math import ceil
import numpy as np
import pandas as pd
import os

import calendar
import psycopg2
import random

class IdpProfiles:
    
    def __init__(self,df_index, **kwargs):
        
        self.df = pd.DataFrame(index = df_index)
        self.annual_heat_demand=kwargs.get('annual_heat_demand')
        self.temperature = kwargs.get('temperature')
    
   #combination of weighted_temperature, get_normalized_bdew and temperature_inteval
    def get_temperature_interval(self, how='geometric_series'):
        """Appoints the corresponding temperature interval to each temperature
        in the temperature vector.
        """
        self.df['temperature']=self.temperature.values        
        
        temperature =self.df['temperature'].resample('D').mean().reindex(
            self.df.index).fillna(method='ffill').fillna(method='bfill')
        
        if how == 'geometric_series':
            temperature_mean = (temperature + 0.5 * np.roll(temperature, 24) +
                                    0.25 * np.roll(temperature, 48) +
                                    0.125 * np.roll(temperature, 72)) / 1.875
        elif how == 'mean':
            temperature_mean = temperature
        
        else:
            temperature_mean = None
        
        self.df['temperature_geo']= temperature_mean
        
        temperature_rounded=[]
        
        for i in self.df['temperature_geo']: 
            temperature_rounded.append(ceil(i))
        
        intervals = ({
            -20: 1, -19: 1, -18: 1, -17: 1, -16: 1, -15: 1, -14: 2,
            -13: 2, -12: 2, -11: 2, -10: 2, -9: 3, -8: 3, -7: 3, -6: 3, -5: 3,
            -4: 4, -3: 4, -2: 4, -1: 4, 0: 4, 1: 5, 2: 5, 3: 5, 4: 5, 5: 5,
            6: 6, 7: 6, 8: 6, 9: 6, 10: 6, 11: 7, 12: 7, 13: 7, 14: 7, 15: 7,
            16: 8, 17: 8, 18: 8, 19: 8, 20: 8, 21: 9, 22: 9, 23: 9, 24: 9,
            25: 9, 26: 10, 27: 10, 28: 10, 29: 10, 30: 10, 31: 10, 32: 10,
            33: 10, 34: 10, 35: 10, 36: 10, 37: 10, 38: 10, 39: 10, 40: 10})
        
        temperature_interval=[] 
        for i in temperature_rounded:
            temperature_interval.append(intervals[i])
        
        self.df['temperature_interval'] = temperature_interval
        
        return self.df
   
   #importing the entire table from postgres
    def data_from_postgres(self):
        ##after the data is already arranged into a desired format in a new data frame
        db_pgs=pd.DataFrame(index=pd.date_range(pd.datetime(2011, 1, 1, 0),
                                periods=24, freq='H'))
        # connect to the db
        con = psycopg2.connect(host = "localhost",
                               database = "sdm_try",
                                user="postgres", password="egon_2020")          
        # cursor
        cur = con.cursor()
        #executr the querry
        cur.execute("SELECT * FROM sample_idp")
        rows = cur.fetchall()
        columns = list(range(366))
        for c in columns:
            x=[]
            for r in rows:
                x.append(r[c])
            db_pgs[c]=x
        cur.close()
        #close the connection
        con.close()
        db_pgs.drop(columns=[0],axis=1,inplace=True)
        
        sample_df = db_pgs.copy()
        #idp_hour = db_pgs.sample(axis='columns')
                
        return sample_df
        #return db_pgs                     

    def random_idp(self):
    
        db_pgs = self.data_from_postgres()
        
        temperature_int = self.get_temperature_interval(
            how = 'geometric_series')['temperature_interval']
        temperature_interval = temperature_int.resample('D').max()
        
        idp_list=[]
        for temp_class in temperature_interval[0:365]: #which days are needed
            if temp_class == 4:
                low = 1
                high = 61
            elif temp_class == 5:
                low = 61
                high = 121
            elif temp_class == 6:
                low = 121
                high = 181
            elif temp_class == 7:
                low = 181
                high = 241
            elif temp_class == 8:
                low = 241
                high = 301
            elif temp_class == 9:
                low = 301
                high = 365           
            idp_hour = db_pgs[random.randint(low,high)]
            idp_hour = idp_hour.tolist()
            idp_list.append(idp_hour)

        final_idp=[]
        for daily_profile in idp_list:
            for idp in daily_profile:
            	final_idp.append(idp)
        
        idp_dic={'Heat Demand':final_idp}
        
        profile = pd.DataFrame.from_dict(idp_dic)
        
        return profile         
    
    def random_idp_original(self):
        db_pgs = self.data_from_postgres()
        idp_hour = db_pgs[random.randint(1,365)]
        #idp_hour = self.get_from_post_gres.sample(axis='columns')
        
        return idp_hour
        
    def get_idp(self,filename='sample_normalized.csv'):
        file = os.path.join(self.datapath, filename)
        idp= pd.read_csv(file, index_col=0)
        
        return idp
    
    def get_profile(self):
        return self.get_idp() * self.annual_heat_demand 