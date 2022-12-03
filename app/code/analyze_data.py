from pyspark.sql.functions import col, lit, row_number, when
from pyspark.sql import Window

def process(spark, config):
  
    primary_person_path = config['data']['primary_person_path']
 
    units_path = config['data']['units_path']
 

    person_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(primary_person_path).\
        select(col('CRASH_ID'), col('PRSN_INJRY_SEV_ID'), col('DEATH_CNT'), col('PRSN_GNDR_ID'), col('DRVR_LIC_STATE_ID'), col('PRSN_ETHNICITY_ID'),
              col('PRSN_ALC_RSLT_ID'), col('DRVR_ZIP'))


    unit_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(units_path).select(col('UNIT_DESC_ID'), col('CRASH_ID'), col('VEH_MAKE_ID'), col('VEH_BODY_STYL_ID'),
                                                                                                  col('VEH_DMAG_SCL_1_ID'), col('VEH_DMAG_SCL_2_ID'))
    
    analysis_1(person_df)

    analysis_2(unit_df)
    
    analysis_3(person_df)
    
    analysis_4(person_df, unit_df)
    
    analysis_5(person_df, unit_df)
    
    analysis_6(person_df)
    
    analysis_7(unit_df)
    
    
                                                                                                  
def analysis_1(person_df):
    person_df = person_df.select(col('PRSN_GNDR_ID'), col('DEATH_CNT'), col('PRSN_INJRY_SEV_ID'))
    
    num_crashes = person_df.filter((col('PRSN_GNDR_ID')==lit('MALE')) & (col('DEATH_CNT') == lit(1)) & ( col('PRSN_INJRY_SEV_ID')==lit('KILLED') )).count()
    print("number of crashes in which number of persons killed are male: ", num_crashes)
    

def analysis_2(unit_df):
    unit_df = unit_df.select(col('UNIT_DESC_ID'))
    
    two_wheelers = unit_df.filter(col('UNIT_DESC_ID').isin(['MOTOR VEHICLE', 'PEDALCYCLIST'])).count()
    print("number of two wheelers that are booked for crashes: ", two_wheelers)
    

def analysis_3(person_df):
    person_df = person_df.select(col('PRSN_GNDR_ID'), col('DRVR_LIC_STATE_ID'))
    
    female_df = person_df.filter(col('PRSN_GNDR_ID')==lit('FEMALE'))
    
    print("state having the highest number of accidents in which females are involved: ")
    female_df.select('DRVR_LIC_STATE_ID').groupBy('DRVR_LIC_STATE_ID').count().\
                            withColumnRenamed('count', 'accidents').orderBy(col('accidents').desc()).limit(1).show()
                            
                            
def analysis_4(person_df, unit_df):
    person_df = person_df.select(col('CRASH_ID'), col('PRSN_INJRY_SEV_ID'))
    unit_df = unit_df.select(col('CRASH_ID'), col('VEH_MAKE_ID'))
    
    injured_df = person_df.filter(col('PRSN_INJRY_SEV_ID').isin(['POSSIBLE INJURY', 'KILLED', 'INCAPACITATING INJURY', 'NON-INCAPACITATING INJURY']))
    joined_df = unit_df.join(injured_df, on = ['CRASH_ID'], how= 'inner')
    injuries_per_veh_df = joined_df.select('VEH_MAKE_ID').groupBy('VEH_MAKE_ID').count().\
                                    withColumnRenamed('count', 'no_of_injuries')
    
    w = Window.orderBy(col('no_of_injuries').desc())
    top5_to_15_df = injuries_per_veh_df.withColumn('row_number', row_number().over(w)).\
                                            filter(col('row_number').between(5,15)).drop(col('row_number'))
    
    print("Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death: ")
    
    top5_to_15_df.show()
    
    
def analysis_5(person_df, unit_df):
    
    joined_df = unit_df.join(person_df, on = ['CRASH_ID'], how= 'inner')
    ethnic_cnt_df = joined_df.select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count().\
                                withColumnRenamed('count', 'ethnic_cnt')
    
    w = Window.partitionBy('VEH_BODY_STYL_ID').orderBy('ethnic_cnt')
    top_ethnic_grp_df = ethnic_cnt_df.withColumn('row_number', row_number().over(w)).filter(col('row_number')==lit(1)).drop('ethnic_cnt', 'row_number')
    
    print("For all the body styles involved in crashes, the top ethnic user group of each unique body style: ")
    top_ethnic_grp_df.show()
    
    
def analysis_6(person_df):
    person_df = person_df.select(col('PRSN_ALC_RSLT_ID'), col('DRVR_ZIP'))
    
    zip_cnt_df = person_df.filter(col('PRSN_ALC_RSLT_ID')==lit('Positive')).select('DRVR_ZIP').groupBy('DRVR_ZIP').count().withColumnRenamed('count', 'zip_cnt').orderBy(col('zip_cnt').desc()).limit(5)
    cleaned_zip_cnt_df = zip_cnt_df.withColumn('DRVR_ZIP', when(col('DRVR_ZIP').isNull(), lit('Missing_Zip')).\
                                                            otherwise(col('DRVR_ZIP')))
    
    print("Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash: ")
    cleaned_zip_cnt_df.show()
    
 
def analysis_7(unit_df):
    unit_df = unit_df.select(col('CRASH_ID'), col('VEH_DMAG_SCL_1_ID'), col('VEH_DMAG_SCL_2_ID'))
    
    damage_lvl_df = unit_df.filter(unit_df['VEH_DMAG_SCL_1_ID'].rlike(r'[4-7]'))
    crash_cnt_df = damage_lvl_df.select('CRASH_ID').groupBy('CRASH_ID').count().withColumnRenamed('count', 'crash_cnt')
    
    print("Count of Distinct Crash IDs where No Damaged Property was observed: ")
    crash_cnt_df.show()
    
