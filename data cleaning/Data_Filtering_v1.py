# IMPORT STATEMENTS
import os
import pandas as pd
import datetime
import pandas.testing


# ---- HELPER FUNCTIONS ----
from pandas._testing import assert_frame_equal


def sort_df(df):
    if 'spend_all_inclow' in df.columns:
        df['spend_all_inclow'] = df['spend_all_inclow'].astype(float)
    if 'initial_claims' in df.columns:
        df['initial_claims'] = df['initial_claims'].astype(int)
    if 'total_claims' in df.columns:
        df['total_claims'] = df['total_claims'].astype(int)
    if 'engagement' in df.columns:
        for i in df.columns:
            if df[i].dtype == object:
                df[i] = df[i].astype(float)
    if 'day_endofweek' in df.columns:
        df = df.rename(columns={"day_endofweek": "day"})
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df = df.sort_values(by=['date'])
    if 'Daily' in filename:
        df = daily_to_weekly(df)
    if 'day_endofweek' in df.columns:
        df = df.rename(columns={"day": "day_endofweek"})
    df = df.drop(['date'], axis=1)
    return df


def daily_to_weekly(df):
    """Converts daily data to weekly data to match SafeGraph weeks"""
    if 'cityid' in df.columns and 'merchants_all' in df.columns:
        for column_name in df.columns:
            if 'merchants' in column_name:
                df[column_name] = df[column_name].astype(float)
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'cityid': 'last',
                    'merchants_all': 'last',
                    'merchants_ss40': 'last',
                    'merchants_ss60': 'last',
                    'merchants_ss65': 'last',
                    'merchants_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= '2020-07-13 00:00:00'].reset_index(drop=True)
        return df
    elif 'countyfips' in df.columns and 'merchants_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'countyfips': 'last',
                    'merchants_all': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= '2020-07-13 00:00:00'].reset_index(drop=True)
        return df
    elif 'statefips' in df.columns and 'merchants_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'statefips': 'last',
                    'merchants_all': 'last',
                    'merchants_inchigh': 'last',
                    'merchants_inclow': 'last',
                    'merchants_incmiddle': 'last',
                    'merchants_ss40': 'last',
                    'merchants_ss60': 'last',
                    'merchants_ss65': 'last',
                    'merchants_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= ('2020-07-13 00:00:00')].reset_index(drop=True)
        return df
    elif 'statefips' not in df.columns and 'countyfips' not in df.columns and 'merchants_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'merchants_all': 'last',
                    'merchants_inchigh': 'last',
                    'merchants_inclow': 'last',
                    'merchants_incmiddle': 'last',
                    'merchants_ss40': 'last',
                    'merchants_ss60': 'last',
                    'merchants_ss65': 'last',
                    'merchants_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= ('2020-07-13 00:00:00')].reset_index(drop=True)
        return df
    elif 'cityid' in df.columns and 'revenue_all' in df.columns:
        for column_name in df.columns:
            if 'revenue' in column_name:
                df[column_name] = df[column_name].astype(float)
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'cityid': 'last',
                    'revenue_all': 'last',
                    'revenue_ss40': 'last',
                    'revenue_ss60': 'last',
                    'revenue_ss65': 'last',
                    'revenue_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= ('2020-07-13 00:00:00')].reset_index(drop=True)
        return df
    elif 'countyfips' in df.columns and 'revenue_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'countyfips': 'last',
                    'revenue_all': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= ('2020-07-13 00:00:00')].reset_index(drop=True)
        return df
    elif 'statefips' in df.columns and 'revenue_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'statefips': 'last',
                    'revenue_all': 'last',
                    'revenue_inchigh': 'last',
                    'revenue_inclow': 'last',
                    'revenue_incmiddle': 'last',
                    'revenue_ss40': 'last',
                    'revenue_ss60': 'last',
                    'revenue_ss65': 'last',
                    'revenue_ss70': 'last',
                    'date': 'last'
                    }
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= ('2020-07-13 00:00:00')].reset_index(drop=True)
        return df
    else:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'revenue_all': 'last',
                    'revenue_inchigh': 'last',
                    'revenue_inclow': 'last',
                    'revenue_incmiddle': 'last',
                    'revenue_ss40': 'last',
                    'revenue_ss60': 'last',
                    'revenue_ss65': 'last',
                    'revenue_ss70': 'last',
                    'date': 'last'
                    }
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        df = df[df['date'] <= '2020-07-13 00:00:00'].reset_index(drop=True)
        return df


def name_files(df, filename):
    '''This function takes a DataFrame and converts it into a unique csv with a unique name'''
    # df.to_csv('temp\{}'.format(filename), index = False)
    if 'Affinity' in filename:
        filename = filename.replace('Affinity', 'Consumer Spending')
    if 'Burning Glass' in filename:
        filename = filename.replace('Burning Glass', 'Job Postings')
    if 'UI Claims' in filename:
        filename = filename.replace('UI Claims', 'Unemployement Claims')
    if 'Womply Merchants' in filename:
        filename = filename.replace('Womply Merchants', 'Small Businesses Open')
    if 'Womply Revenue' in filename:
        filename = filename.replace('Womply Revenue', 'Small Businesses Revenue')
    if 'Zearn' in filename:
        filename = filename.replace('Zearn', 'Online Math Learning')
    if 'Daily' in filename:
        filename = filename.replace('Daily', 'Weekly')
    df.to_csv('C:/Users/spate/Downloads/Track the Recovery/filtered_data/{}'.format(filename), index=False)


def last_week_is_same(df, filename):
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/temp', filename)
    last_week_df = pd.read_csv(path)
    last_week_size = len(last_week_df)

    for i in df.columns:
        if df[i].dtype == 'int32':
            df[i] = df[i].astype('int64')
        if df[i].dtype == 'object':
            df[i] = df[i].astype('float64')

    try:
        assert_frame_equal(df.reset_index(drop=True).iloc[:last_week_size], last_week_df)
        df.to_csv('C:/Users/spate/Downloads/Track the Recovery/temp/{}'.format(filename), index=False)
        #         print(df.reset_index(drop = True).iloc[:last_week_size])
        #         print(last_week_df)
        return True
    except:
        return False


# ---- HELPER FUNCTIONS ----

def sd_city(filename):
    '''Filters Economic Tracker city level data to only include San Diego County'''
    sd_fips_code = 6
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    city = pd.read_csv(path)
    city = city[city['cityid'] == sd_fips_code]
    city = sort_df(city)
    if last_week_is_same(city, filename):
        name_files(city, filename)
    else:
        print('Last week\'s data not in the same format as this week\'s')


def sd_county(filename):
    '''Filters Economic Tracker county level data to only include San Diego County'''
    sd_fips_code = 6073
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    county = pd.read_csv(path)
    county = county[county['countyfips'] == sd_fips_code]
    # not all csv files have unsorted dates, but this is in case they ever do
    county = sort_df(county)
    if last_week_is_same(county, filename):
        name_files(county, filename)
    else:
        print('Last week\'s data not in the same format as this week\'s')


def state_ca(filename):
    """ Filters Economic Tracker state level data to only include California """
    ca_fips_code = 6
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    state = pd.read_csv(path)
    state = state[state['statefips'] == ca_fips_code]
    state = sort_df(state)
    if last_week_is_same(state, filename):
        name_files(state, filename)
    else:
        print('Last week\'s data not in the same format as this week\'s')


def us_national(filename):
    """Moves Economic Tracker national level data to filtered_data folder"""
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    us = pd.read_csv(path)
    us = sort_df(us)
    if last_week_is_same(us, filename):
        name_files(us, filename)
    else:
        print('Last week\'s data not in the same format as this week\'s')


# RUN THIS STATEMENT
count = 0
for filename in os.listdir("C:/Users/spate/Downloads/Track the Recovery/raw_data"):
    print(os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename))
    count += 1

    if 'City' in filename:
        sd_city(filename)
    if 'County' in filename:
        sd_county(filename)
    elif 'State' in filename:
        state_ca(filename)
    elif 'National' in filename:
        us_national(filename)

print(count)
