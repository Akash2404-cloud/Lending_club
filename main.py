from utility.file_functions.get_csv_file import *
from configs.config import *
from utility.file_functions.extras import *
from utility.file_functions.write_file import *
from utility.file_functions.table_join import *
from pyspark.sql.functions import *


df = get_file(input_file ,  True , False)
df.show(10 , truncate = False)

ids = set_ids(id_cols)
new_df = df.withColumn("name_sha2", ids )

# --------------------------------------------------------------------------------------------------------------
customer_df = new_df.select( expr('name_sha2 as member_id') , 'emp_title','emp_length','home_ownership','annual_inc','addr_state',
'zip_code', expr("'USA' as country"),'grade','sub_grade','verification_status','tot_hi_cred_lim','application_type','annual_inc_joint'
,'verification_status_joint')
write_cleanedfiles = write_file(loc = files['customers']['bronze'], data_format = 'csv')
write_cleanedfiles.file_writer(customer_df , 'overwrite')

loans_df = new_df.select(expr('id as loan_id , name_sha2 as member_id'),'loan_amnt','funded_amnt','term','int_rate',
'installment','issue_d','loan_status','purpose','title' )
write_cleanedfiles = write_file(loc = files['loans']['bronze'], data_format = 'csv')
write_cleanedfiles.file_writer(loans_df , 'overwrite')

loans_repayments_df = new_df.select( expr('id as loan_id'),'total_rec_prncp','total_rec_int','total_rec_late_fee',
'total_pymnt','last_pymnt_amnt','last_pymnt_d','next_pymnt_d')
write_cleanedfiles = write_file(loc = files['loans_repayments']['bronze'], data_format = 'csv')
write_cleanedfiles.file_writer(loans_repayments_df , 'overwrite')

loans_defaulters_df = new_df.select( expr('name_sha2 as member_id'),'delinq_2yrs','delinq_amnt','pub_rec',
'pub_rec_bankruptcies','inq_last_6mths','total_rec_late_fee','mths_since_last_delinq','mths_since_last_record')
write_cleanedfiles = write_file(loc = files['loan_deafulters']['bronze'], data_format = 'csv')
write_cleanedfiles.file_writer(loans_defaulters_df , 'overwrite')


# -----------------------------------------------------------------------------------------------------------------

customers = get_file(files['customer']['bronze'] , False , schema = customer_schema)
customer = customers.withColumnRenamed("annual_inc", "annual_income") \
.withColumnRenamed("addr_state", "address_state") \
.withColumnRenamed("zip_code", "address_zipcode") \
.withColumnRenamed("country", "address_country") \
.withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
.withColumnRenamed("annual_inc_joint", "join_annual_income")

customers = add_time(customers).where('annual_income is not null') \
    .withColumn('emp_length',regexp_replace(col("emp_length"), "(\D)","")) \
    .withColumn('emp_length' , customers['emp_length'].cast(int)) \
    .withColumn('address_state' , when(expr('length(address) > 2' , 'NA')) \
                .otherwise(customers['address_state']))

avg_emp_len = customer_df.select(expr('avg(emp_length)')).collect()[0][0]
# customers = customers.fillna({'emp_length':avg_emp_len})
customers = customers.na.fill(avg_emp_len , subset = ['emp_length'])
write_cleanedfiles = write_file(loc = files['customers']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(customers , 'overwrite')



loans = get_file(files['loans']['bronze'] , False , schema = loans_schema)
loans = add_time(loans)
columns_to_check = ["loan_amount", "funded_amount", "loan_term_months",
                    "interest_rate", "monthly_installment",
                    "issue_date", "loan_status", "loan_purpose"]
loans = loans.na.drop(subset=columns_to_check)
loans = loans.withColumn("loan_term_months",(regexp_replace(col("loan_term_months"), " months", "") \
.cast("int") / 12) .cast("int")) \
.withColumnRenamed("loan_term_months","loan_term_years")

loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase",
                       "medical", "small_business", "car", "vacation", "moving", "house", "wedding",
                       "renewable_energy", "educational"]
loans = loans.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup),
                                              col("loan_purpose")).otherwise("other"))
write_cleanedfiles = write_file(loc = files['loans']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')



loans_repayments = get_file(files['loans_repayments']['bronze'] , False , schema = loans_repay_schema)
loans_repayments = add_time(loans_repayments)
columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received",
                    "total_payment_received", "last_payment_amount"]
loans_repayments = loans_repayments.na.drop(subset=columns_to_check)
loans_repayments = loans_repayments.withColumn("total_payment_received",
    when((col("total_principal_received") != 0.0) & (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")) \
        .otherwise(col("total_payment_received")))
loans_repayments = loans_repayments.withColumn("last_payment_date", when((col("last_payment_date") == 0.0),
       None).otherwise(col("last_payment_date")))
loans_repayments = loans_repayments.withColumn("next_payment_date",when((col("next_payment_date") == 0.0),
       None).otherwise(col("next_payment_date")))
write_cleanedfiles = write_file(loc = files['loans_repayments']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')



loans_defaulters = get_file(files['loans_defaulters']['bronze'] , False , schema = loan_defaulters_schema)
loans_defaulters = add_time(loans_defaulters)
loans_defaulters = loans_defaulters.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")) \
                  .fillna(0, subset = ["delinq_2yrs"])
write_cleanedfiles = write_file(loc = files['loans_defaulters']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')

loans_defaulters_delinq = loans_defaulters.where('delinq_2yrs > 0 or mths_since_last_delinq > 0') \
    .select('member_id','delinq_2yrs' , 'delinq_amnt' , expr('int(mths_since_last_delinq)'))
write_cleanedfiles = write_file(loc = files['loans_defaulters_delinq']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')


loans_defaulters_records_enq = loans_defaulters \
    .where('pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0')
write_cleanedfiles = write_file(loc = files['loans_defaulters_records_enq']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')


loans_defaulters_detail_records_enq = loans_defaulters.select('member_id ,pub_rec, pub_rec_bankruptcies , inq_last_6mths')
loans_defaulters_detail_records_enq = loans_defaulters_detail_records_enq.withColumn("pub_rec", col("pub_rec").cast("integer")) \
    .fillna(0, subset = ["pub_rec"]) \
    .withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")) \
    .fillna(0, subset = ["pub_rec_bankruptcies"]) \
    .withColumn("inq_last_6mths", col("inq_last_6mths").cast("integer")) \
    .fillna(0, subset = ["inq_last_6mths"])
write_cleanedfiles = write_file(loc = files['loans_defaulters_detail_records_enq']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')

# --------------------------------------------------------------------------------------------------------------------------------

total_tables = join_tables(customers , loans , loans_repayments ,
                loans_defaulters_delinq , loans_defaulters_detail_records_enq)
write_cleanedfiles = write_file(loc = files['total_files'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')