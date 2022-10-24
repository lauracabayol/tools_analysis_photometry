#!/usr/bin/env python
# encoding: UTF8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from sqlalchemy.engine import create_engine
from sqlalchemy import *
from sqlalchemy.orm import *

engine = create_engine('postgresql://readonly@db03.pau.pic.es/dm')


class pairwise_test:
    
    def __init__(self, input_file=None,input_df=None,field=None,production_id=None, subsample_size=None):
        """Pairwise comparison of single-exposure photometry.
        :param input_file: {str} Path to input file. The file must include the columns \
        'flux', 'flux_error','image_id and 'ref_id'
        :param input_df: {DataFrame} DataFrame with the photometry. The DataFrame must \
        include the columns 'flux', 'flux_error','image_id and 'ref_id'. If neither the input_file
        nor the input_df are provided provided, the data will be queried to the database.
        :param field: {str} PAUS field to query.
        :param production_id: {int}  PAUS photometry production id. \
        If neither field or input field is provided, the production id is required.
        :param subsample_size: {int} Randomly sub-sample the photometry before \ 
        running the pairwise comparison
        """
        
        self.field=field
        self.input_file=input_file
        self.input_df = input_df
        self.production_id=production_id
        self.dict_prod = dict(cosmos=948,w1=956, w3=958 ,w4=959)
        self.subsample_size = subsample_size
        
        
        if self.input_file==None:
            self.engine = create_engine('postgresql://readonly@db03.pau.pic.es/dm')
            
    def load_data(self):
        if self.input_df is not None:
            print('Data already provided!')
            df = self.input_df 
        if self.input_file is not None:
            print('Loading the data from file')
            df = pd.read_csv(self.input_file,sep=',', header = 0)
        elif (self.input_df is None)&(self.input_file is None):
            #assert((self.production_id is None)&(self.field is None)), "You Must provide either the field or the production id!"
            print('Querying the data base')
            if self.production_id:
                production_id = self.production_id
            else:
                production_id = self.dict_prod[self.field]
                
                     
            query = f"""SELECT fa.ref_id, fa.flux, fa.image_id, fa.flux_error, i.filter
                    from forced_aperture as fa
                    JOIN image as i ON i.id=fa.image_id
                    WHERE fa.production_id=%s"""%(production_id)
            

            with self.engine.begin() as conn:
                conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
                df = pd.read_sql(query, conn)
                
        return df
        
    def load_zpcalib(self):
        query = f"""SELECT i.id, i.filter,izp.zp,izp.zp_error
                FROM image as i
                JOIN image_zp as izp ON izp.image_id = i.id
                WHERE izp.phot_method_id = 2
                AND izp.calib_method = 'MBE2.1_xsl'""" 

        with engine.begin() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
            df = pd.read_sql(query, conn)
            df = df.rename(columns = {'id':'image_id','filter':'band'})

        return df

    def calib_data(self,dat,calibzp):
        dat = dat.merge(calibzp, on = 'image_id')

        var_cal =  dat.flux_error**2*dat.zp**2 + dat.zp_error**2*dat.flux**2 \
        + dat.flux_error**2 * dat.zp_error**2 
        flux_cal = dat.zp * dat.flux            


        dat['flux_cal'] = flux_cal
        dat['flux_error_cal'] = np.sqrt(var_cal)

        return dat
        
        
    def calculate_pairwise(self):

        dat = self.load_data()
        calibzp = self.load_zpcalib()

        dat = self.calib_data(dat,calibzp)

        if self.subsample_size:
            dat = dat.sample(self.subsample_size)

        pairs = dat.merge(dat, on=['ref_id','band'])
        pairs = pairs[pairs.image_id_x != pairs.image_id_y]

        std = np.sqrt(pairs.flux_error_cal_x.pow(2) + pairs.flux_error_cal_y.pow(2))
        X_pairs = (pairs.flux_cal_y - pairs.flux_cal_x) / std

        pairs['X'] = X_pairs

        return pairs


    def plots(self,pairs1, pairs2, hist=True):

        if hist:
            plt.hist(pairs1.X,bins =50,density =True)
            x = np.linspace(-3,3, 100)
            plt.plot(x, stats.norm.pdf(x, 0, 1))
            plt.grid()
            plt.xlabel(r'($f_{\rm 1} - f_{\rm 2})^2\ /\ (\sigma_{\rm 1} - \sigma_{\rm 2}$)',fontsize = 14)
            plt.ylabel('Frequency')
            plt.xticks(fontsize = 12)
            plt.yticks(fontsize = 12)
            plt.show()

        return




            