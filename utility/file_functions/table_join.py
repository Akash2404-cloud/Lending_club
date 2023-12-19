from utility.logger import *
# from utility.file_functions.get_csv_file import *
# from configs.config import *

def join_tables(spark , customers , loans , loans_repayments ,
                loans_defaulters_delinq , loans_defaulters_detail_records_enq):


    c1 = customers.member_id == loans.member_id
    c2 = loans.member_id == loans_repayments.member_id
    c3 = customers.member_id == loans_defaulters_delinq.member_id
    c4 = customers.member_id == loans_defaulters_detail_records_enq.member_id

    logger.info('creating joined tables')

    total_tables = customers.join(loans , on = c1 , how = 'inner') \
    .join(loans_repayments , on = c2 , how = 'inner') \
    .join(loans_defaulters_delinq , on = c3 , how = 'inner') \
    .join(loans_defaulters_detail_records_enq , on = c4 , how = 'inner' )

    return total_tables


