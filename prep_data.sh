echo "download latest covid data"
curl -s -d /dev/null https://docs.google.com/spreadsheets/d/1lCV0fuiBPspAfOh2gDHgJ4nBPXYFQGZpTSDZah1vi2k/export?exportFormat=csv > ~/projects/covid/who_global_indicators.csv
curl -s -d /dev/null https://docs.google.com/spreadsheets/d/1avGWWl1J19O_Zm0NGTGy2E-fOG05i4ljRfjl87P7FiA/export?exportFormat=csv > ~/projects/covid/tableau_covid_sourced_from_jhu.csv
ls -alh ~/projects/covid/
python etl_data.py
