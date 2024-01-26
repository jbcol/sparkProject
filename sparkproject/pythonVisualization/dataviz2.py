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

        dict_pop_per_dptm = {}
        with open('../../popdep.csv', 'r+') as file:
                for line in file:
                        line = line.split(';')
                        dict_pop_per_dptm[line[0]] = int(line[1].replace('\n', ''))

        list_per = [0]*96
        with open(f'../../csvTotPopVaccinedTime/{csv_filename}', 'r+') as file:
                for line in file:
                        line = line.split(';')
                        if line[0] in codes_dptm:
                                list_per[dict_dptm[line[0]]] = float(line[1].replace(',', '.'))/dict_pop_per_dptm[line[0]]
                        if line[0] == '28': #corsica 2A
                                list_per[28] = float(line[1].replace(',', '.'))/dict_pop_per_dptm[line[0]]
                        if line[0] == '29': #corsica 2B
                                list_per[29] = float(line[1].replace(',', '.'))/dict_pop_per_dptm[line[0]]
        sf['value'] = list_per


        deps = gv.Polygons(sf, vdims=['nom','value'])
        deps.opts(width=600, height=600, toolbar='above', color=dim('value'), 
                colorbar=True, tools=['hover'], aspect='equal')

        gv.output(fig='html')
        gv.save(deps, "./time/"+html_filename)

        #add a h1 centered in the page with the title
        with open("./time/"+html_filename, 'r+') as file:
                content = file.read()
                file.seek(0, 0)
                file.write(f'<div style="display: flex; justify-content: center; align-items: center;"><h1 style="text-align: center;width: 50%;place-self: center;">{title}</h1></div>\n')
                file.write(content)



plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2021-04-27)', 'totVaccinedByDepPeriode1.html', 'totVaccinedByDepPeriode1.csv')
plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2021-08-27)', 'totVaccinedByDepPeriode2.html', 'totVaccinedByDepPeriode2.csv')
plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2021-12-27)', 'totVaccinedByDepPeriode3.html', 'totVaccinedByDepPeriode3.csv')
plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2022-04-27)', 'totVaccinedByDepPeriode4.html', 'totVaccinedByDepPeriode4.csv')
plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2022-08-27)', 'totVaccinedByDepPeriode5.html', 'totVaccinedByDepPeriode5.csv')
plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2022-12-27)', 'totVaccinedByDepPeriode6.html', 'totVaccinedByDepPeriode6.csv')
plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2023-04-27)', 'totVaccinedByDepPeriode7.html', 'totVaccinedByDepPeriode7.csv')
plotMap('Pourcentage de la population vaccinée (vaccins tout confondus) sur la période covid (2020-12-27 to 2023-07-10)', 'totVaccinedByDepPeriode8.html', 'totVaccinedByDepPeriode8.csv')

#gdf = gpd.read_file("departements-version-simplifiee.geojson")
#gdf.plot()
#plt.show()