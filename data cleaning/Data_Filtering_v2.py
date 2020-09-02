# IMPORT STATEMENTS
import os
import pandas as pd
import datetime
import pandas.testing
from pandas.testing import assert_frame_equal

# ---- START HELPER FUNCTIONS ----


def clean_df(df):
    """Corrects the type of certain columns to be uniform, sorts the date of the dataframes, and
    convert daily data to weekly data"""
    # convert columns to non-object types to be more uniform
    # UI Claims
    if 'initial_claims' in df.columns:
        df['initial_claims'] = df['initial_claims'].astype(int)
    if'total_claims' in df.columns:
        df['total_claims'] = df['total_claims'].astype(int)
    # Zearn
    if 'engagement' in df.columns:
        for i in df.columns:
            if df[i].dtype == object:
                df[i] = df[i].astype(float)
    # Womply Merchants
    if 'cityid' in df.columns and 'merchants_all' in df.columns:
        for column_name in df.columns:
            if 'merchants' in column_name:
                df[column_name] = df[column_name].astype(float)

    # sort by the date
    if 'day_endofweek' in df.columns:
        df = df.rename(columns={"day_endofweek": "day"})
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df = df.sort_values(by=['date'])

    # turn daily data into weekly data
    if 'Daily' in filename and 'Womply' in filename:
        df = womply_daily_to_weekly(df)
    if 'Daily' in filename and 'Womply' not in filename:
        if 'Affinity' in filename:
            # the values before this date on the raw dataframes were blank
            df = df[df['date'] >= '2020-01-13 00:00:00']
        df = affinity_daily_to_weekly(df)

    if 'day_endofweek' in df.columns:
        df = df.rename(columns={"day": "day_endofweek"})
    df = df.drop(['date'], axis=1)
    return df


def womply_daily_to_weekly(df):
    """Converts daily data to weekly data from Womply to match SafeGraph weeks"""
    if 'cityid' in df.columns and 'merchants_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'cityid': 'last',
                    'merchants_all': 'last',
                    'merchants_ss40': 'last',
                    'merchants_ss65': 'last',
                    'merchants_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
        return df
    elif 'countyfips' in df.columns and 'merchants_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'countyfips': 'last',
                    'merchants_all': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
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
                    'merchants_ss65': 'last',
                    'merchants_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
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
                    'merchants_ss65': 'last',
                    'merchants_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
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
                    'revenue_ss65': 'last',
                    'revenue_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
        return df
    elif 'countyfips' in df.columns and 'revenue_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'countyfips': 'last',
                    'revenue_all': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
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
                    'revenue_ss65': 'last',
                    'revenue_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
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
                    'revenue_ss65': 'last',
                    'revenue_ss70': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
        return df


def affinity_daily_to_weekly(df):
    """Converts daily data to weekly data from Affinity to match SafeGraph weeks. Also checks
    if any other files have been converted to weekly in the process."""
    if 'cityid' in df.columns and 'spend_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'cityid': 'last',
                    'spend_acf': 'last',
                    'spend_aer': 'last',
                    'spend_all': 'last',
                    'spend_apg': 'last',
                    'spend_grf': 'last',
                    'spend_hcs': 'last',
                    'spend_tws': 'last',
                    'date': 'last'
                   }
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
        return df
    elif 'countyfips' in df.columns and 'spend_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'countyfips': 'last',
                    'spend_all': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
        return df
    elif 'statefips' in df.columns and 'spend_all' in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'statefips': 'last',
                    'spend_acf': 'last',
                    'spend_aer': 'last',
                    'spend_all': 'last',
                    'spend_apg': 'last',
                    'spend_grf': 'last',
                    'spend_hcs': 'last',
                    'spend_tws': 'last',
                    'spend_all_inchigh': 'last',
                    'spend_all_inclow': 'last',
                    'spend_all_incmiddle': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
        return df
    elif 'statefips' not in df.columns and 'countyfips' not in df.columns and 'cityid' not in df.columns:
        agg_dict = {'year': 'last',
                    'month': 'last',
                    'day': 'last',
                    'spend_acf': 'last',
                    'spend_aer': 'last',
                    'spend_all': 'last',
                    'spend_apg': 'last',
                    'spend_grf': 'last',
                    'spend_hcs': 'last',
                    'spend_tws': 'last',
                    'spend_all_inchigh': 'last',
                    'spend_all_inclow': 'last',
                    'spend_all_incmiddle': 'last',
                    'date': 'last'}
        df = df.resample('W-Mon', on='date').agg(agg_dict)
        if df.tail(1).index[0] == df['date'].tail(1)[0]:
            df = df[df['date'] <= df['date'].tail(1)[0]].reset_index(drop=True)
        else:
            df = df[df['date'] <= df['date'].tail(2)[0]].reset_index(drop=True)
        return df
    if 'spend_all_inclow' in df.columns:
        df['spend_all_inclow'] = df['spend_all_inclow'].astype(float)
    else:
        # Indicates that a file has changed
        raise ValueError('New file converted from weekly to daily')


def compare_rows(col1, col2, column_name):
    combined_df = pd.DataFrame({("(new) " + str(column_name)): col1, ("(old) " + str(column_name)): col2})

    if col1.dtype == 'int64' and column_name not in ['year', 'month', 'day', 'cityid', 'countyfips', 'statefips']:
        combined_df['difference'] = (-500 <= combined_df["(new) " + str(column_name)] - combined_df["(old) " +
                                                                                                    str(column_name)]) \
                                    & (combined_df["(new) " + str(column_name)] - combined_df["(old) " +
                                                                                              str(column_name)] <= 500)
    else:
        combined_df['difference'] = (-0.8 <= combined_df["(new) " + str(column_name)] - combined_df["(old) " +
                                                                                                    str(column_name)]) \
                                    & (combined_df["(new) " + str(column_name)] - combined_df["(old) " +
                                                                                              str(column_name)] <= 0.8)
    if False in list(combined_df['difference']):
        print(combined_df)
        raise ValueError("Larger than 0.01 difference between columns")


def last_week_is_same(df, filename):
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/temp', filename)
    last_week_df = pd.read_csv(path)
    last_week_size = len(last_week_df)

    for i in df.columns:
        if df[i].dtype == 'int32':
            df[i] = df[i].astype('int64')
        if df[i].dtype == 'object':
            df[i] = df[i].astype('float64')

    for i in df.columns:
        # noinspection PyTypeChecker
        if False in list(df.columns == last_week_df.columns):
            raise ValueError("The columns have changed")
        else:
            try:
                compare_rows(df.reset_index(drop=True).iloc[:last_week_size][i], last_week_df.iloc[:last_week_size][i], i)
            except ValueError("The rows are not the same"):
                return False

    return True


def name_files(df, filename):
    """This function takes a DataFrame and converts it into a unique csv with a unique name"""
    # df.to_csv('temp\{}'.format(filename), index = False)
    if 'Affinity' in filename:
        filename = filename.replace('Affinity', 'Consumer Spending')
    if 'Burning Glass' in filename:
        filename = filename.replace('Burning Glass', 'Job Postings')
    if 'UI Claims' in filename:
        filename = filename.replace('UI Claims', 'Unemployment Claims')
    if 'Womply Merchants' in filename:
        filename = filename.replace('Womply Merchants', 'Small Businesses Open')
    if 'Womply Revenue' in filename:
        filename = filename.replace('Womply Revenue', 'Small Businesses Revenue')
    if 'Zearn' in filename:
        filename = filename.replace('Zearn', 'Online Math Learning')
    if 'Daily' in filename:
        filename = filename.replace('Daily', 'Weekly')
    df.to_csv('C:/Users/spate/Downloads/Track the Recovery/filtered_data/{}'.format(filename), index=False)

# ---- END HELPER FUNCTIONS ----


def sd_city(filename):
    """Filters Economic Tracker city level data to only include San Diego County"""
    sd_fips_code = 6
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    city = pd.read_csv(path)
    city = city[city['cityid'] == sd_fips_code]
    city = clean_df(city)
    if last_week_is_same(city, filename):
        city.to_csv('C:/Users/spate/Downloads/Track the Recovery/temp/{}'.format(filename), index=False)
        if 'day' in city.columns:
            print(city.tail(1)[['month', 'day']])
        elif 'day_endofweek' in city.columns:
            print(city.tail(1)[['month', 'day_endofweek']])
        else:
            print('There is day column')
        name_files(city, filename)
    else:
        raise ValueError('Last week\'s data not in the same format as this week\'s\n')


def sd_county(filename):
    """Filters Economic Tracker county level data to only include San Diego County"""
    sd_fips_code = 6073
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    county = pd.read_csv(path)
    county = county[county['countyfips'] == sd_fips_code]
    # not all csv files have unsorted dates, but this is in case they ever do
    county = clean_df(county)
    if last_week_is_same(county, filename):
        county.to_csv('C:/Users/spate/Downloads/Track the Recovery/temp/{}'.format(filename), index=False)
        if 'day' in county.columns:
            print(county.tail(1)[['month', 'day']])
        elif 'day_endofweek' in county.columns:
            print(county.tail(1)[['month', 'day_endofweek']])
        else:
            print('There is day column')
        name_files(county, filename)
    else:
        raise ValueError('Last week\'s data not in the same format as this week\'s\n')


def state_ca(filename):
    """Filters Economic Tracker state level data to only include California"""
    ca_fips_code = 6
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    state = pd.read_csv(path)
    state = state[state['statefips'] == ca_fips_code]
    state = clean_df(state)
    if last_week_is_same(state, filename):
        state.to_csv('C:/Users/spate/Downloads/Track the Recovery/temp/{}'.format(filename), index=False)
        if 'day' in state.columns:
            print(state.tail(1)[['month', 'day']])
        elif 'day_endofweek' in state.columns:
            print(state.tail(1)[['month', 'day_endofweek']])
        else:
            print('There is day column')
        name_files(state, filename)
    else:
        raise ValueError('Last week\'s data not in the same format as this week\'s\n')


def us_national(filename):
    """Moves Economic Tracker national level data to filtered_data folder"""
    path = os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename)
    us = pd.read_csv(path)
    us = clean_df(us)
    if last_week_is_same(us, filename):
        us.to_csv('C:/Users/spate/Downloads/Track the Recovery/temp/{}'.format(filename), index=False)
        if 'day' in us.columns:
            print(us.tail(1)[['month', 'day']])
        elif 'day_endofweek' in us.columns:
            print(us.tail(1)[['month', 'day_endofweek']])
        else:
            print('There is day column')
        name_files(us, filename)
    else:
        raise ValueError('Last week\'s data not in the same format as this week\'s\n')

# --- RUN THIS STATEMENT ---
for filename in os.listdir("C:/Users/spate/Downloads/Track the Recovery/raw_data"):

    print(os.path.join('C:/Users/spate/Downloads/Track the Recovery/raw_data', filename))

    if 'City' in filename:
        sd_city(filename)
    if 'County' in filename:
        sd_county(filename)
    elif 'State' in filename:
        state_ca(filename)
    elif 'National' in filename:
        us_national(filename)