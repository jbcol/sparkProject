import geopandas as gpd
from geoviews import dim
import geoviews as gv
import numpy as np
import matplotlib.pyplot as plt

def plotMap(title, html_filename, csv_filename):
        sf = gpd.read_file('departements-version-simplifiee.geojson')

        codes_dptm = []
        for i in range(0, len(sf)):
                codes_dptm.append(sf['code'][i])

        #make a dictionnary with the code of the departement the index of the line
        dict_dptm = {}
        for i in range(0, len(sf)):
                dict_dptm[sf['code'][i]] = i

        list_per = [0]*96
        with open(f'../../{csv_filename}', 'r+') as file:
                for line in file:
                        line = line.split(';')
                        if line[0] in codes_dptm:
                                list_per[dict_dptm[line[0]]] = float(line[1].replace(',', '.'))
                        if line[0] == '28': #corsica 2A
                                list_per[28] = float(line[1].replace(',', '.'))
                        if line[0] == '29': #corsica 2B
                                list_per[29] = float(line[1].replace(',', '.'))

        sf['value'] = list_per


        deps = gv.Polygons(sf, vdims=['nom','value'])
        deps.opts(width=600, height=600, toolbar='above', color=dim('value'), 
                colorbar=True, tools=['hover'], aspect='equal')

        gv.output(fig='html')
        gv.save(deps, html_filename)

        #add a h1 centered in the page with the title
        with open(html_filename, 'r+') as file:
                content = file.read()
                file.seek(0, 0)
                file.write(f'<div style="display: flex; justify-content: center; align-items: center;"><h1 style="text-align: center;width: 50%;place-self: center;">{title}</h1></div>\n')
                file.write(content)



plotMap('Pourcentage de la population vaccinée par département avec le vaccin 1 (pfizer) sur la période covid (2020-12-27 to 2023-07-10)', 'percentageVaccin1ByDep.html', 'percentageVaccin1ByDep.csv')
plotMap('Pourcentage de la population vaccinée par département avec le vaccin 2 (moderna) sur la période covid (2020-12-27 to 2023-07-10)', 'percentageVaccin2ByDep.html', 'percentageVaccin2ByDep.csv')

plotMap('Pourcentage de la population ayant un schema vaccinal 1 (pfizer) complet par département sur la période covid (2020-12-27 to 2023-07-10)', 'percentageSchemaComplet1ByDep.html', 'percentageSchemaComplet1ByDep.csv')
plotMap('Pourcentage de la population ayant un schema vaccinal 2 (moderna) complet par département sur la période covid (2020-12-27 to 2023-07-10)', 'percentageSchemaComplet2ByDep.html', 'percentageSchemaComplet2ByDep.csv')

plotMap('Pourcentage de la population ayant fait le 1er rappel du vaccin 1 (pfizer) par département sur la période covid (2020-12-27 to 2023-07-10)', 'percentageRappel1ByDep.html', 'percentageRappel1ByDep.csv')
plotMap('Pourcentage de la population ayant fait le 1er rappel du vaccin 2 (moderna) par département sur la période covid (2020-12-27 to 2023-07-10)', 'percentageRappel2ByDep.html', 'percentageRappel2ByDep.csv')

#gdf = gpd.read_file("departements-version-simplifiee.geojson")
#gdf.plot()
#plt.show()