import geopandas as gpd
from geoviews import dim
import geoviews as gv
import numpy as np
import matplotlib.pyplot as plt

sf = gpd.read_file('departements-version-simplifiee.geojson')
sf['value'] = np.random.randint(1, 10, sf.shape[0]) #tableau de 1 à 10, taille de sf.shape[0]
deps = gv.Polygons(sf, vdims=['nom','value'])

deps.opts(width=600, height=600, toolbar='above', color=dim('value'), 
        colorbar=True, tools=['hover'], aspect='equal')

gv.output(fig='html')
gv.save(deps, 'dataviz.html')

