"""COMBINE scraped-data"""
import pandas as pd
# file ini dipakai untuk menggabungkan beberapa pecahan
# data yang sudah di scrape dari folder scraped-data
result_profile_sekolah = pd.concat(map(pd.read_csv, [
    'scraped-data\school_dapodik_profiles_0_1_2023-09-25.csv',
    'scraped-data\school_dapodik_profiles_0_2023-09-24.csv',
    'scraped-data\school_dapodik_profiles_2_5_2023-09-25.csv',
    'scraped-data\school_dapodik_profiles_6_10_2023-09-25.csv',
    'scraped-data\school_dapodik_profiles_11_15_2023-09-25.csv',
    'scraped-data\school_dapodik_profiles_16_20_2023-09-25.csv',
    'scraped-data\school_dapodik_profiles_21_25_2023-09-29.csv',
    'scraped-data\school_dapodik_profiles_26_2023-10-08.csv',
    'scraped-data\school_dapodik_profiles_27_2023-10-08.csv',
    'scraped-data\school_dapodik_profiles_28_30_2023-10-08.csv'
]))

result_profile_sekolah.to_csv('./data-result/school_dapodik_profile_progress_4.csv', index=False)