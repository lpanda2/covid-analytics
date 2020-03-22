import pandas as pd
import numpy as np
import os
pd.options.display.max_columns = 50
pd.options.display.width = 200
pd.options.display.max_rows = 500

# convienience functions
def time_formatter():
    return pd.datetime.now().strftime('%Y_%m_%d_%H_%m')


def diff(col):
    return (col - col.shift(1)).fillna(0).astype(int)

# source data
covid = pd.read_csv('tableau_covid_sourced_from_jhu.csv')
who = pd.read_csv('who_global_indicators.csv')
median_age = pd.read_csv('gapminder_median_age_years.csv', usecols=['country', '2020'])
pop_density = pd.read_csv('gapminder_population_density_per_square_km.csv', usecols=['country', '2020'])
pop_age_group = pd.read_csv('ourworldindata_population_by_broad_age_group.csv')

# clean source data
name_replace = {
    'Bahamas': 'Bahamas, The',
    'Cape Verde': 'Cabo Verde',
    'Czech Republic': 'Czechia',
    'Gambia': 'Gambia, The',
    'Kyrgyz Republic': 'Kyrgyzstan',
    'South Korea': 'Korea, South',
    'United States': 'US'
}

median_age['Country_Region'] = median_age.country.replace(name_replace)
median_age.rename(columns={'2020': 'Median_Age'}, inplace=True)
median_age.drop('country', axis=1, inplace=True)

pop_density['Country_Region'] = pop_density.country.replace(name_replace)
pop_density.rename(columns={'2020': 'Pop_Density'}, inplace=True)
pop_density.drop('country', axis=1, inplace=True)

pop_age_group = pop_age_group.loc[pop_age_group.Year == 2015]
pop_age_group['Country_Region'] = pop_age_group.Entity.replace(name_replace)
pop_age_group.drop('Entity', axis=1, inplace=True)
pop_age_group.columns = ['Country_Code', 'Year', 'Pop_Under_5', 'Pop_15_24', 'Pop_25_64', 'Pop_65_Older', 'Pop_5_14', 'Country_Region']
pop_cols = [x for x in pop_age_group if ('Pop' in x) and ('Percent' not in x)]
pop_age_group['Sum'] = pop_age_group[pop_cols].sum(axis=1)
for i in pop_cols:
    pop_age_group[f'{i}_Percent'] = pop_age_group[i] / pop_age_group[pop_cols].sum(axis=1)

covid = covid.fillna('NA')
covid['Date'] = pd.to_datetime(covid['Date'])
covid['Latest_Date'] = pd.to_datetime(covid['Latest_Date'])
covid = covid.sort_values(['Country_Region', 'Province_State', 'Case_Type', 'Date']).reset_index(drop=True)
covid_country = covid.groupby(['Date', 'Country_Region', 'Case_Type']).agg({'Cases': 'sum', 'Latest_Date': 'max', 'Lat': 'mean', 'Long': 'mean'}).reset_index()
covid_country['Difference'] = covid_country.groupby(['Country_Region'])['Cases'].transform(diff)

# pivot
def pivot(covid, state_level=False):
    index = ['Country_Region', 'Province_State'] if state_level else ['Country_Region']
    covid_pivot = pd.pivot_table(covid, index=index + ['Date'], columns=['Case_Type'], values=['Cases'], aggfunc='max').reset_index()
    covid_pivot.columns = index + ['Date', 'Case_Active', 'Case_Confirmed', 'Case_Deaths', 'Case_Recovered']

    covid_pivot['Total_Active'] = covid_pivot.groupby(index)['Case_Active'].transform('max')
    covid_pivot['Total_Active'] = np.where(covid_pivot['Total_Active'] == covid_pivot['Case_Active'], covid_pivot['Total_Active'], 0)
    covid_pivot['Difference_Active'] = covid_pivot.groupby(index)['Case_Active'].transform(diff)

    covid_pivot['Total_Deaths'] = covid_pivot.groupby(index)['Case_Deaths'].transform('max')
    covid_pivot['Total_Deaths'] = np.where(covid_pivot['Total_Deaths'] == covid_pivot['Case_Deaths'], covid_pivot['Total_Deaths'], 0)
    covid_pivot['Difference_Deaths'] = covid_pivot.groupby(index)['Case_Deaths'].transform(diff)
    
    covid_pivot['Total_Recovered'] = covid_pivot.groupby(index)['Case_Recovered'].transform('max')
    covid_pivot['Total_Recovered'] = np.where(covid_pivot['Total_Recovered'] == covid_pivot['Case_Recovered'], covid_pivot['Total_Recovered'], 0)
    covid_pivot['Difference_Recovered'] = covid_pivot.groupby(index)['Case_Recovered'].transform(diff)

    covid_pivot['Max_Confirmed'] = covid_pivot.groupby(index)['Case_Confirmed'].transform('max')
    
    covid_pivot['Total_Confirmed'] = np.where(covid_pivot['Max_Confirmed'] == covid_pivot['Case_Confirmed'], covid_pivot['Max_Confirmed'], 0)
    covid_pivot['Death_Rate_Empirical'] = covid_pivot['Case_Deaths'] / covid_pivot.groupby(index)['Case_Confirmed'].transform('max')
    assert (covid_pivot['Case_Confirmed'] == covid_pivot[['Case_Active', 'Case_Deaths', 'Case_Recovered']].sum(axis=1)).all()

    covid_pivot['Day_Index'] = covid_pivot['Date'] - covid_pivot.groupby(index)['Date'].transform(min)
    covid_pivot['Day_Index'] = covid_pivot['Day_Index'].dt.days

    case = (covid_pivot['Case_Confirmed'].shift(1) < 100) & (covid_pivot['Case_Confirmed'] > 100)
    min_row = covid_pivot.groupby(index)['Case_Confirmed'].transform(min)
    case |= (min_row > 100) & (covid_pivot['Case_Confirmed'] > 100)
    case &= covid_pivot['Country_Region'] == covid_pivot['Country_Region'].shift(1)
    covid_pivot['Date_100thCase'] = np.where(case, covid_pivot['Date'].dt.strftime('%Y-%m-%d'), '')
    covid_pivot['Date_100thCase'] = pd.to_datetime(covid_pivot['Date_100thCase'])
    covid_pivot['Days_Since_100'] = covid_pivot['Date'] - covid_pivot.groupby(index)['Date_100thCase'].transform(min)
    covid_pivot['Days_Since_100'] = covid_pivot['Days_Since_100'].dt.days
    covid_pivot.drop('Date_100thCase', axis=1, inplace=True)

    # fix data for china
    china = covid_pivot['Country_Region'] == 'China'
    covid_pivot['Days_Since_100'] = np.where(china, covid_pivot['Days_Since_100'] + 15, covid_pivot['Days_Since_100'])

    func = lambda x: pd.Series.rank(x, pct=True)
    covid_pivot['Percent_Rank'] = covid_pivot.groupby('Date')['Case_Confirmed'].transform(func)

    return covid_pivot


if __name__ == '__main__':

    # pivot initial table
    covid_country_pivot = pivot(covid_country)

    # merges
    covid_country_with_who = covid_country_pivot.merge(who, how='left', left_on=['Country_Region'], right_on='Country', indicator=True)
    covid_country_with_who['Percent_Infected'] = covid_country_with_who['Case_Confirmed'] / covid_country_with_who['Population_total']
    covid_country_with_who['Max_Percent_Infected'] = covid_country_with_who['Total_Confirmed'] / covid_country_with_who['Population_total']

    # merge median_age, pop_density, pop_age_group
    covid_country_with_who = covid_country_with_who.merge(median_age, how='left', on='Country_Region')
    covid_country_with_who = covid_country_with_who.merge(pop_density, how='left', on='Country_Region')
    covid_country_with_who = covid_country_with_who.merge(pop_age_group, how='left', on='Country_Region')

    # write out data
    os.makedirs('output', exist_ok=True)
    covid_country_pivot.to_csv(f'output/lp_covid_country_pivot_{time_formatter()}.csv', index=False)
    covid_country_pivot.to_csv(f'output/lp_covid_country_pivot.csv', index=False)
    covid_country_with_who.to_csv(f'output/lp_covid_country_with_who_{time_formatter()}.csv', index=False)
    covid_country_with_who.to_csv(f'output/lp_covid_country_with_who.csv', index=False)
