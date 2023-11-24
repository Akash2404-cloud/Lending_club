
# properties_spark = {
# 'spark.sql.unacceptable_rated_pts' :  0,
# 'spark.sql.very_bad_rated_pts' : 100,
# 'spark.sql.bad_rated_pts':  250,
# 'spark.sql.good_rated_pts' :  500,
# 'spark.sql.very_good_rated_pts' :  650,
# 'spark.sql.excellent_rated_pts' : 800,
# 'spark.sql.unacceptable_grade_pts' : 750,
# 'spark.sql.very_bad_grade_pts' : 1000,
# 'spark.sql.bad_rated_grade_pts' : 1500,
# 'spark.sql.good_grade_pts' : 2000,
# 'spark.sql.very_good_grade_pts' : 2500
# }

id_cols = ["emp_title", "emp_length", "home_ownership", "annual_inc",
           "zip_code", "addr_state", "grade", "sub_grade",
          "verification_status"]

customer_schema = '''member_id string, emp_title string, emp_length string, home_ownership string, 
annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, 
verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, 
verification_status_joint string'''

loans_schema = '''loan_id string, member_id string, loan_amount float, funded_amount float, 
                loan_term_months string, interest_rate float, monthly_installment float, issue_date string,
                loan_status string, loan_purpose string, loan_title string'''

loans_repay_schema = '''loan_id string, total_principal_received float, total_interest_received float, 
                      total_late_fee_received float, total_payment_received float, last_payment_amount float, 
                      last_payment_date string, next_payment_date string'''

loan_defaulters_schema = """member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, 
pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, 
mths_since_last_record float"""


columns_to_check_loan = ["loan_amount", "funded_amount", "loan_term_months",
                    "interest_rate", "monthly_installment",
                    "issue_date", "loan_status", "loan_purpose"]

loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase",
                       "medical", "small_business", "car", "vacation", "moving", "house", "wedding",
                       "renewable_energy", "educational"]

columns_to_check_repayments = ["total_principal_received", "total_interest_received", "total_late_fee_received",
                    "total_payment_received", "last_payment_amount"]

basic_properties_csv = {
    'header':'true',
    'mode':'permissive'
}

input_file = 'D:\\p_proj\\accepted_2007_to_2018Q4.csv'

files = {
    'customer' : {
        'bronze' : 'D:\\p_proj\\bronze\\customers',
        'silver' : 'D:\\p_proj\\silver\\customers',
        'gold' : 'D:\\p_proj\\gold\\customers'
    },
    'loans' : {
        'bronze': 'D:\\p_proj\\bronze\\loans',
        'silver': 'D:\\p_proj\\silver\\loans',
        'gold': 'D:\\p_proj\\gold\\loans'
    },
    'loans_repayments':{
        'bronze': 'D:\\p_proj\\bronze\\loans_repayments',
        'silver': 'D:\\p_proj\\silver\\loans_repayments',
        'gold': 'D:\\p_proj\\gold\\loans_repayments'
    },
    'loans_defaulters':{
        'bronze': 'D:\\p_proj\\bronze\\loans_defaulters',
        'silver': 'D:\\p_proj\\silver\\loans_defaulters',
        'gold': 'D:\\p_proj\\gold\\loans_defaulters'
    },
    'loans_defaulters_delinq':{
        'silver': 'D:\\p_proj\\silver\\loans_defaulters_delinq',
        'gold': 'D:\\p_proj\\gold\\loans_defaulters_delinq'
    },
    'loans_defaulters_records_enq': {
        'silver': 'D:\\p_proj\\silver\\loans_defaulters_records_enq'
    },
    'loans_defaulters_detail_records_enq':{
        'silver': 'D:\\p_proj\\silver\\loans_defaulters_detail_records_enq'
    },
    'total_files': 'D:\\p_proj\\gold\\total_files' ,
    'bad_data': 'D:\\p_proj\\bad_files'

}