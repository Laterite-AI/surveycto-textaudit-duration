import pandas as pd
import numpy as np
import os

###############################################################################
## Set-up:
    
#specify name of dataset relative to the folder path
data= ''

#Specify name of excel version of coded CTO relative to folder path
instrument_xlsx = ''

#specify name of the output that is going to be outputed relative to the folder path
output_xlsx = ''

#specify the path to the media folder with the / at the end
media_path = ''


## Load dataset
df_data_wide = pd.read_csv(data, low_memory=False)


## Load survey instrument
df_instrument = pd.read_excel(instrument_xlsx, sheet_name='survey')

## List timing csvs and creating a dataframe with names of text audit files 
df_lu = pd.DataFrame([file for file in os.listdir(media_path) if file[-4:]=='.csv'], columns=['csv'])
df_lu['KEY'] = 'uuid:' + df_lu['csv'].str[3:-4]
df_lu = pd.merge(df_lu, df_data_wide[['KEY']], how='inner', on='KEY')

#creating a list of dataframes from all the text audit files
list_df_timing = [pd.read_csv(media_path + file) for file in df_lu['csv']]
list_df_timing = [df.assign(KEY=key) for (key, df) in zip(df_lu['KEY'], list_df_timing)]

###############################################################################
## Functions:

def gather_timings(list_df_timings):
    """"This function takes in a list of dataframe of all the text audit files and 
    returns a unified dataframe with new columns added in for variable names, group and 
    repeat group names. Also returns a list of names for the newly added columns"""
    df = pd.concat(list_df_timings)
    df.reset_index(drop=True, inplace=True)
    df['grouplist'] = df['Field name'].str.split('/')
    df['question'] = df['grouplist'].apply(lambda x: x[-1])
    df['grouplist'] = df['grouplist'].apply(lambda x: x[:-1])
    max_nested = df['grouplist'].map(lambda x: len(x)).max()
    levels = ['module' +str(i+1) for i in range(max_nested)] + ['question']

    for i in range(max_nested):
        df['module' + str(i+1)] = df['grouplist'].apply(lambda x: get_item(x, i))
        df['module' + str(i+1)] = df['module' + str(i+1)].str.replace('\[\d+]','')
    df.drop(['grouplist', 'Field name'], axis=1, inplace=True)    

    for i in range(max_nested-1):
        assert (list(set(df['module' + str(i+1)]) & set(df['module'+ str(i+2)])) == ['']) | (list(set(df['module' + str(i+1)]) & set(df['module'+ str(i+2)])) == [])
    return df, levels

def get_item(x, level):
    try:
        x = x[level]
    except:
        x = '' 
    return x    

def pivot_timings(df_input, levels):
    """This functions gives the variable names for groups and repeat groups(as well as just question)
    that will be used by the pivot_level function to get statistics and organizes the output."""
    nest_levels = [levels[:i+1] for i in range(len(levels))]
    df = pd.concat([pivot_level(df_input, level ) for level in nest_levels], sort=True)
    df[levels] = df[levels].fillna('')
    df.drop_duplicates(subset=levels, inplace=True)
    df['name'] = df.apply(lambda x: get_varname(x, levels), axis=1)
    return df

def pivot_level(df_input, level):
    """This function uses the newly added columns to get total duration per submission 
    for all the groups and repeat groups and after doing that for all submissions
    calls other functions to get summary statistics"""
    df_input = df_input[['Total duration (seconds)',
       'First appeared (seconds into survey)', 'KEY'] + level]
    df_input = df_input.groupby(['KEY'] + level).sum()
    for i in range(len(level)):
        df_input.reset_index(level=len(level)-i, drop=False, inplace=True)
    df_input.reset_index(drop=False, inplace=True)
    df = pd.pivot_table(df_input, values='Total duration (seconds)', index=level, aggfunc=[np.count_nonzero, np.mean, np.std, np.median, np.min, p10, p25, p75, p90, np.max])
    df.columns = df.columns.droplevel(level=1)
    df.reset_index(inplace=True)
    return df

def p10(x):
    return np.percentile(x, 10)
def p25(x):
    return np.percentile(x, 25)
def p75(x):
    return np.percentile(x, 75)
def p90(x):
    return np.percentile(x, 90)

def get_varname(x, levels):
    name = ''
    i = 1
    while name == '' and i <= len(levels):
        name = x[levels[-i]] 
        i +=1
    return name

def pivot_order(df_pivot, df_instrument, levels):
    """This function merges the final output with the instrument to order the variables in the 
    order they appear in the instrument"""
    df_instrument = df_instrument[['name']]
    df_instrument = df_instrument.drop_duplicates()
    df_instrument.reset_index(inplace=True)
    df_instrument.rename(columns={'index':'row'}, inplace=True)
    df = pd.merge(df_pivot, df_instrument, how='inner', on='name')
    df.sort_values('row', inplace=True)
    df['module'] = np.where(df['question']=='', df['name'], '')
    df = df[['row', 'module'] + [levels[-1]] + ['count_nonzero', 'mean', 'std', 'amin', 'p10', 'p25', 'median', 'p75', 'p90', 'amax'] + levels[:-1]]
    return df
###############################################################################

# create timing statistics:
df_timings, group_levels = gather_timings(list_df_timing)

# mark fieldlists:
_modules = [c for c in df_timings.columns if c.startswith('module')]
_cols = ['KEY', 'Total duration (seconds)', 'First appeared (seconds into survey)'] + _modules


df_timings['fieldlist'] = 1
for c in _cols:
    df_timings['fieldlist'] = df_timings['fieldlist'] * (df_timings[c].shift() == df_timings[c])

# taking care of questions in field-lists so that we do not take into acount twice
df_timings['Total duration (seconds)'] = np.where(df_timings['fieldlist'] == 0,
           df_timings['Total duration (seconds)'], 0)
        
df_timings_stats = pivot_timings(df_timings, group_levels)
df_timings_stats = pivot_order(df_timings_stats, df_instrument, group_levels)

# format times for excel in seconds:
# changing the units for calcuation from default seconds to minutes
df_timings_stats[['mean', 'std', 'amin', 'p10', 'p25', 'median', 'p75', 'p90', 'amax']] = df_timings_stats[['mean', 'std', 'amin', 'p10', 'p25', 'median', 'p75', 'p90', 'amax']] /(24*60*60)


#renaming columns names as per the changes
df_timings_stats.rename(columns={'mean':'mean (hh:mm:ss)','std':'std (hh:mm:ss)',
                                 'amin':'amin (hh:mm:ss)','p10':'p10 (hh:mm:ss)',
                                 'p25':'p25 hh:mm:ss)','median': 'median (hh:mm:ss)', 
                                  'p75':'p75 (hh:mm:ss)', 'p90':'p90 (hh:mm:ss)',
                                  'amax':'amax (hh:mm:ss)'}, inplace=True)

#saving final output
df_timings_stats.to_excel(output_xlsx, index=False)