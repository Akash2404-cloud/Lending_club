from utility.file_functions.get_csv_file import *
from configs.config import *
from utility.file_functions.extras import *
from utility.file_functions.write_file import *
from utility.file_functions.table_join import *
from pyspark.sql.functions import *
from utility.creating_spark_session import *

spark = get_spark_session()

df = get_file(spark,input_file ,  True , False)
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

customers = get_file(spark ,files['customer']['bronze'] , False , schema = customer_schema)
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



loans = get_file(spark , files['loans']['bronze'] , False , schema = loans_schema)
loans = add_time(loans).na.drop(subset=columns_to_check_loan) \
        .withColumn("loan_term_months",(regexp_replace(col("loan_term_months"), " months", "") \
        .cast("int") / 12) .cast("int")) \
        .withColumnRenamed("loan_term_months","loan_term_years") \
        .withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup),
                                              col("loan_purpose")).otherwise("other"))
write_cleanedfiles = write_file(loc = files['loans']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')



loans_repayments = get_file(spark , files['loans_repayments']['bronze'] , False , schema = loans_repay_schema)
loans_repayments = add_time(loans_repayments).na.drop(subset=columns_to_check_repayments).withColumn("total_payment_received",
    when((col("total_principal_received") != 0.0) & (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")) \
        .otherwise(col("total_payment_received"))) \
        .withColumn("last_payment_date", when((col("last_payment_date") == 0.0),None).otherwise(col("last_payment_date"))) \
        .withColumn("next_payment_date",when((col("next_payment_date") == 0.0),None).otherwise(col("next_payment_date")))
write_cleanedfiles = write_file(loc = files['loans_repayments']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')



loans_defaulters = get_file(spark , files['loans_defaulters']['bronze'] , False , schema = loan_defaulters_schema)
loans_defaulters = add_time(loans_defaulters).withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")) \
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

total_tables = join_tables(spark ,customers , loans , loans_repayments ,
                loans_defaulters_delinq , loans_defaulters_detail_records_enq)
write_cleanedfiles = write_file(loc = files['total_files'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')

# ----------------------------------------------------------------------------------------------------------------------
bad_customer_ids_1 = customers.groupBy('member_id').agg(count('*').alias('total')) \
    .orderBy(col('total').desc()) \
    .where(col('total') > 1) \
    .select('member_id')

bad_customer_ids_2 = loans_defaulters_delinq.groupBy('member_id').agg(count('*').alias('total')) \
    .orderBy(col('total').desc()).where(col('total') > 1) \
    .select('member_id')

bad_customer_ids_3 = loans_defaulters_detail_records_enq.groupBy('member_id').agg(count('*').alias('total')) \
    .orderBy(col('total').desc()).where(col('total') > 1) \
    .select('member_id')

bad_data_customers = bad_customer_ids_1.union(bad_customer_ids_2) \
    .union(bad_customer_ids_3).distinct()

write_cleanedfiles = write_file(loc = files['bad_data'], data_format = 'csv')
write_cleanedfiles.file_writer(bad_data_customers , 'overwrite')

exclude_customers = bad_data_customers['member_id'].collect()[0][0]
customer = customers.where(~col('member_id').isin(exclude_customers))
write_cleanedfiles = write_file(loc = files['customers']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(customers , 'overwrite')

loans_defaulters_delinq = loans_defaulters_delinq.where(~col('member_id').isin(exclude_customers))
write_cleanedfiles = write_file(loc = files['loans_defaulters_delinq']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')

loans_defaulters_detail_records_enq = loans_defaulters_detail_records_enq.where(~col('member_id').isin(exclude_customers))
write_cleanedfiles = write_file(loc = files['loans_defaulters_detail_records_enq']['silver'], data_format = 'csv')
write_cleanedfiles.file_writer(loans , 'overwrite')

# spark.conf.get('spark.sql.unacceptable_rated_pts')

c1 = loans_repayments.loan_id == loans.loan_id
tb1 = loans_repayments.join(loans ,c1 , 'inner').where(~col('member_id').isin(bad_data_customers))
tb1.select('member_id') \
.withColumn('last_payment_pts' , when( col('last_payment_amount') < (col('monthly_installments') * 0.5) , \
                                       spark.conf.get('spark.sql.very_bad_rated_pts')) \
                                .when( (col('last_payment_amount') > (col('monthly_installments') * 0.5)) and \
                                       ( col('last_payment_amount') < col('monthly_installment') ) \
                                       , spark.conf.get('spark.sql.bad_rated_pts')) \
                                .when( (col('last_payment_amount') == (col('monthly_installments') * 0.5)) ,\
                                       spark.conf.get('spark.sql.good_rated_pts') ) \
                                .when( (col('last_payment_amount') > (col('monthly_installments'))) and \
                                       ( col('last_payment_amount') < (col('monthly_installment') * 1.50) ) , \
                                       spark.conf.get('spark.sql.very_good_rated_pts') ) \
                                .when( (col('last_payment_amount') > (col('monthly_installments') * 1.50)), \
                                       spark.conf.get('spark.sql.excellent_rated_pts')))

ldh_ph_df = spark.sql(''' select p.*
case
when d.delinq_2yrs = 0 then ${spark.sql.excellent_rated_pts}
when d.delinq_2yrs between 1 and 2 then ${spark.sql.bad_rated_pts}
when d.delinq_2yrs between 3 and 5 then ${spark.sql.very_bad_rated_pts}
when d.delinq_2yrs > 5 or d.delinq_2yrs is null then ${spark.sql.unacceptable_rated_pts}
end as delinq_pts ,
case
when l.pub_rec = 0 then ${spark.sql.excellent_rated_pts}
when l.pub_rec between 1 and 2  then ${spark.sql.bad_rated_pts}
when l.pub_rec between 3 and 5 then ${spark.sql.very_bad_rated_pts}
when l.pub_rec > 5 or d.delinq_2yrs is null then ${spark.sql.unacceptable_rated_pts}
end as public_record_pts,
case
when l.pub_rec_bankruptcies = 0 then ${spark.sql.excellent_rated_pts}
when l.pub_rec_bankruptcies between 1 and 2  then ${spark.sql.bad_rated_pts}
when l.pub_rec_bankruptcies between 3 and 5 then ${spark.sql.very_bad_rated_pts}
when l.pub_rec_bankruptcies > 5 or d.delinq_2yrs is null then ${spark.sql.unacceptable_rated_pts}
end as public_bankruptcies_pts,
case
when l.inq_last_6mths = 0 then ${spark.sql.excellent_rated_pts}
when l.inq_last_6mths between 1 and 2  then ${spark.sql.bad_rated_pts}
when l.inq_last_6mths between 3 and 5 then ${spark.sql.very_bad_rated_pts}
when l.inq_last_6mths > 5 or d.delinq_2yrs is null then ${spark.sql.unacceptable_rated_pts}
end as enq_pts
from  lending_club_proj_6879.loans_defaulters_detail_records_enq_new l
inner join lending_club_proj_6879.loans_defaulters_delinq_new d on d.member_id = l.member_id 
inner join ph_pts p on p.member_id = l.member where not l.member_id in (select * from bad_data_customers)
''')

fh_ldh_ph_df = spark.sql(''' select ldef.* ,
case
when lower(l.loan_status) is like '%fully paid%' then ${spark.sql.excellent_rated_pts}
when lower(l.loan_status) is like '%current%' then ${spark.sql.good_rated_pts}
when lower(l.loan_status) is like '%in grace period%' then ${spark.sql.bad_rated_pts}
when lower(l.loan_status) is like '%late (16-30 days)%' or lower(loan_status) is like '%late (31 - 120 days)%' then ${spark.sql.very_bad_rated_pts}
when lower(l.loan_status) is like '%charged off%' then ${spark.sql.unacceptable_rated_pts}
end as loan_status_pts ,
case
when lower(a.home_ownership) like '%own'then ${spark.sql.excellent_rated_pts}
when lower(a.home_ownership) is like '%rent%' then ${spark.sql.good_rated_pts}
when lower(a.home_ownership) is like '%mortgage%' then ${spark.sql.bad_rated_pts}
when lower(a.home_ownership) is like '%any%' or lower(loan_status) is null then ${spark.sql.unacceptable_rated_pts}
end as home_pts
case
when l.funded_amount <= (a.total_high_credit_limit * 0.1) then ${spark.sql.excellent_rated_pts}
when l.funded_amount > (a.total_high_credit_limit * 0.1) and l.funded_amount < (a.total_high_credit_limit * 0.3) then ${spark.sql.good_rated_pts}
when l.funded_amount <= (a.total_high_credit_limit * 0.3) and l.funded_amount < (a.total_high_credit_limit * 0.5) then ${spark.sql.bad_rated_pts}
when l.funded_amount <= (a.total_high_credit_limit * 0.5) and l.funded_amount < (a.total_high_credit_limit * 0.9) then ${spark.sql.very_bad_rated_pts}
when l.funded_amount = a.total_high_credit_limit then ${spark.sql.unacceptable_rated_pts}
end as funded_amt_pts
case
when a.grade = 'A' and a.sub_grade in ('A1','A2' , 'A3' , 'A4' , 'A5') then ${spark.sql.excellent_rated_pts}
when a.grade = 'B' and a.sub_grade in ('B1','B2' , 'B3' , 'B4' , 'B5') then ${spark.sql.good_rated_pts}
when a.grade = 'C' and a.sub_grade in ('C1','C2' , 'C3' , 'C4' , 'C5') then ${spark.sql.bad_rated_pts}
when a.grade = 'D' and a.sub_grade in ('D1','D2' , 'D3' , 'D4' , 'D5') then ${spark.sql.very_bad_rated_pts}
when a.grade = 'E' and a.sub_grade in ('E1','E2' , 'E3' , 'E4' , 'E5') then ${spark.sql.unacceptable_rated_pts}
end as grade_pts
from ldh_ph_pts ldef
inner join lending_club_proj_6879.loans l on ldef.member_id = l.member_id 
inner join lending_club_proj_6879.customer_new a on a.member_id = ledf.member_id 
''')

loan_score_df = spark.sql('''
select ((last_payment_pts + total_payment_received_pts) * 0.2 ) as payment_history_pts ,
((delinq_pts + public_record_pts + public_bankruptcies_pts + enq_pts) * 0.45) as loan_default_history_pts,
((loan_status_pts + home_pts + funded_amt_pts + grade_pts) * 0.35) as c
from fh_ldh_ph_pts
''')

loan_score_df = loan_score_df.withColumn('Loan_total_score' , loan_score_df.payment_history_pts + \
                                         loan_score_df.loan_default_history_pts + loan_score_df.loan_score_df )

loan_score_eval = spark.sql('''
select ls.* ,
case
when ls.Loan_total_score >= ${'spark.sql.very_good_grade_pts'} then A+
when ls.Loan_total_score between ${'spark.sql.good_grade_pts'} and ${'spark.sql.very_good_grade_pts'}-1 then B
when ls.Loan_total_score betwneen ${'spark.sql.bad_rated_grade_pts'} and  ${'spark.sql.good_grade_pts'}-1  then C
when ls.Loan_total_score between ${'spark.sql.very_bad_grade_pts'}  and ${'spark.sql.bad_rated_grade_pts'}-1 then D
when ls.Loan_total_score between ${'spark.sql.unacceptable_grade_pts'} and ${'spark.sql.very_bad_grade_pts'} -1 then E
when ls.Loan_total_score < ${'spark.sql.unacceptable_grade_pts'} then F
end as loan_grade
from final_loan_score ls
''')