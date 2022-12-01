

def process(spark, config):
    charges_path = config['data']['charges_path']
    damages_path = config['data']['damages_path']
    endorse_path = config['data']['endorse_path']
    primary_person_path = config['data']['primary_person_path']
    restrict_path = config['data']['restrict_path']
    units_path = config['data']['units_path']

    charges_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(charges_path)
    damages_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(damages_path)
    endorse_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(endorse_path)
    primary_person_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(primary_person_path)
    restrict_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(restrict_path)
    units_df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(units_path)






