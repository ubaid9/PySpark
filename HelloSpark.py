import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4j
import traceback
from lib.utils import get_spark_app_conf, load_survey_df,count_by_country
#this is the change

'''
asdfnsdsfbsdflsdlfoifndjnsdf
dsfjsdhf;sdhfjsdhfjsdhf
# Tis is commit2
# Tis is commit3
# bbb

'''
#made this
if __name__ == "__main__":
    try:
        conf = get_spark_app_conf()  # method 3
        # conf=SparkConf() #Method2:
        # conf.set("spark.app.name","Hello Spark") #Method2:
        # conf.set("spark.master","local[3]") #Method2:

        # spark=SparkSession.builder.appName("UbaidApp").master("local[3]").getOrCreate() #Method1: hardcode method

        spark = SparkSession.builder.config(conf=conf).getOrCreate()  # Method2 & 3: more appropriate method below

        logger = Log4j(spark)
        if len(sys.argv)!=2:
            logger.error("Usage is not correct. HelloSpark <filename>")
            sys.exit(-1)

        # print(logger.sc)
        # for i in logger.sc:
        #     print(i[0],'=',i[1])
        # conf_out=spark.sparkContext.getConf()
        # logger.info(conf_out.toDebugString())
        logger.info("Starting HelloSpark")
        # Your processing code

        #Method 1 to read a dataFrame
        # survey_df=spark.read \
        #     .option("header","true") \
        #     .option("inferSchema","true") \
        #     .sys.argv[1]

        #method2 using function from utils.py
        survey_df=load_survey_df(spark,sys.argv[1])
        partitioned_survey_df=survey_df.repartition(2)

        count_df=count_by_country(partitioned_survey_df)
        logger.info(count_df.collect()) #Instead of show() we used collect()
        print('1=====================================================')

        #this is method 1 in method 2 we will move the code to a function
        # count_df2=survey_df \
        # .where("Age < 40")\
        # .select("Age","Gender","Country","state")\
        # .groupBy("Country")\
        # .count()
        # count_df2.show()

        print('2=====================================================')
        # survey_df.show() # Action to show the dataFrame which was lazily evaluated in above statement
        logger.info("Finished HelloSpark")
        input("Press ENterer")

    except Exception as err:
        print("Something went wrong",err)
        print(traceback.format_exc())

    finally:
        spark.stop()


