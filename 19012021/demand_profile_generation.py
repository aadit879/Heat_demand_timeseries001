# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 14:25:03 2021

@author: Aadit
"""
import pandas as pd
import os
import numpy as np
import bdew_sample as bdew
import matplotlib.pyplot as plt

datapath = os.path.join(os.path.dirname(__file__), 'Temperature_006_ERA5_2mtemp.csv')
temperature_K = pd.read_csv(datapath)

index = pd.date_range(pd.datetime(2010, 1, 1, 0), periods=8760, freq='H')

demand = pd.DataFrame(index=index)


temperature_K.rename(columns={'Unnamed: 0':'Time'}, inplace =True)
temperature_K['Time']=pd.to_datetime(temperature_K['Time'])
temperature_K.set_index('Time', inplace = True)

for column_name in temperature_K:
               
        temperature = temperature_K[column_name]-273
        
        demand[column_name]=bdew.IdpProfiles(
            index, temperature=temperature).random_idp().values
    
        ax = demand.plot()
        ax.set_xlabel("Date")
        ax.set_ylabel("Heat demand in kW {}".format(column_name))
        plt.show()
        
        print('Annual consumption {}: \n{}'.format(column_name, demand[column_name].sum()))
        
        
        



