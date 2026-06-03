from logger_config import logger
from datetime import datetime
import time


def execution_time(func):

    def wrapper(*args,**kwargs):

        start=datetime.now()

        result=func(*args,**kwargs)

        end=datetime.now()

        logger.info(f"time to execute {func.__name__}: {end-start} sec")

        return result


    return wrapper


def manage_connection(connection_func): # recieves parameter

    def decorator(func): # recieves actual fucntion

        def wrapper(*args,**kwargs):

            conn=None 

            try:
                logger.info("opening db connection..")
                conn=connection_func()
                result=func(conn,*args,**kwargs)

                return  result
            
            finally:
                if conn:

                    logger.info(" closing db connection..")
                    conn.close()
    
        return wrapper
    
    return decorator



def retry(limits=3):

    def decorator(func):

        def wrapper(*args,**kwargs):
            counter=0

            while(counter<limits):


                try:
                    result=func(*args,**kwargs)
                    logger.info(f"{func.__name__} successfull retry decorator")
                    return result

                except Exception as e:

                    logger.warning(f"failed attempt :{counter+1} for {func.__name__} :{e}")
                    logger.info(f"retrying {counter+1} time after 2 sec..")
                    counter=counter+1
                    time.sleep(2)
                    
            raise Exception(f"{func.__name__}failed after {limits} attempts")
        
        return wrapper
    
    return decorator
















       
        


    
   