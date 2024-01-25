import geopandas as gpd
import matplotlib.pyplot as plt


gdf = gpd.read_file("departements-version-simplifiee.geojson")
gdf.plot()
plt.show()