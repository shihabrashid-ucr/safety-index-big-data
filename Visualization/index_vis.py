import geopandas as gpd
import geoplot as gplt
import geoplot.crs as gcrs
import matplotlib.pyplot as plt
import mapclassify as mc


# load the data
nyc_boroughs = gpd.read_file('MapPlot.geojson')


fig = plt.figure(figsize=(200,100))
proj = projection=gcrs.AlbersEqualArea()
ax1 = plt.subplot(121, projection=proj)
scheme = mc.Quantiles(nyc_boroughs['Safety_Index'], k=10)

ax1 = gplt.choropleth(
    nyc_boroughs, hue='Safety_Index', projection=proj,
    edgecolor='white', linewidth=1,
    cmap='inferno_r',
    legend=True, legend_kwargs={'loc': 'upper left'},
    scheme=scheme
)
ax1.set_title("Safety Index of NYC")

plt.savefig("NY Heatmap.eps", format='eps', bbox_inches='tight', pad_inches=0, dpi=1200)