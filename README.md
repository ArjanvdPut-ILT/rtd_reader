# rtd_reader
RisKeer (voorheen RingToets) data omzetten naar fileformaat wat direct in een GIS te gebruiken is.

Gebruikt pandas en geopandas voor dataframes en geodataframes

## Gebruiks Voorbeeld
```
import os
import rtd_reader

rtd_file = <pad naar een RisKeer database (*.rtd)>
rtd = rtd_reader.RTD(rtd_file)

for table_name in rtd.list_table_names():
  # Print informatie over de tabel
  print(rtd.table_info(table_name))
  
  # Schrijf tabel naar csv file met de pandas methode .to_csv() 
  csv_filename = <pad voor output csv>
  dataframe = rtd.table_to_df(table_name)
  dataframe.to_csv(csv_filename)
  
  # Schrijf GeoJSON voor elke xml kolom in de tabel
  # NB. Elke kolomnaam die eindigt op 'xml' wordt omgezet ongegacht of het RD coordinaten
  # of lokale coordinaten van b.v. een dijkprofiel zijn. Het laatste geval is in een gis niet 
  # bruikbaar.
  for xml_col in rtd.list_geo_xml_columns(table_name):
    geojson_filename = <pad voor output geojson>
    geodataframe = rtd.table_to_geodf(table_name, xml_col, geotype='line')
    
    if geodataframe.shape[0] > 0:
      gdf.to_file(geojson_filename, driver='GeoJSON')
``` 
