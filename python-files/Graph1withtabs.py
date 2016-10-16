
# coding: utf-8

# In[ ]:

# geo_map_world() functions in this notebook is referenced from the notebook created by Dan Liu
# the addition of states geographic information in the source dictionary is also referenced from same
# the link to the notebook is in the next line
# https://apsportal.ibm.com/analytics/notebooks/82160445-33a4-47ee-9d15-deaa058ae460/view?projectid=aaf120f6-78d3-4028-8235-2d56e431c278


# In[1]:

# add sqlContext
sqlContext = SQLContext(sc)


# In[2]:

# import required packages
from bokeh.io import output_notebook, push_notebook, show
from bokeh.models.widgets import Panel, Tabs
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    ColorBar, LinearColorMapper, FixedTicker, NumeralTickFormatter, BasicTicker,
    Callback, Select, CustomJS,
    LabelSet, TapTool
)
from bokeh.plotting import figure
from bokeh.sampledata.us_states import data as states_dict
import numpy as np, requests, json, pandas as pd, itertools


# In[3]:

# function to create world map coordinates
def get_geo_world():
    url = 'https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json'
    r = requests.get(url)
    geo_json_data = r.json()
    features = geo_json_data['features']
    depth = lambda L: isinstance(L, list) and max(map(depth, L))+1
    xs = []
    ys = []
    names = []
    for feature in features:
        name = feature['properties']['name']
        coords = feature['geometry']['coordinates']
        nbdims = depth(coords)
        # one border
        if nbdims == 3:
            pts = np.array(coords[0], 'f')
            xs.append(pts[:, 0])
            ys.append(pts[:, 1])
            names.append(name)
        # several borders
        else:
            for shape in coords:
                pts = np.array(shape[0], 'f')
                xs.append(pts[:, 0])
                ys.append(pts[:, 1])
                names.append(name)
    source = ColumnDataSource(data=dict(x = xs, y = ys, name = names,))
    return source


# In[4]:

# function to extract data from required tables
def get_data():
    
    occGrpSal = pd.DataFrame()
    # save the link to connect to dashDB
    url = "jdbc:db2://50.97.93.115:50000/BLUDB:user=dash015183;password=9D6Fk9i1h942;"
    
    # extract the required tables from dashDB
    stateareas = sqlContext.read.jdbc(url, 'STATEAREAS')
    occu_grp = sqlContext.read.jdbc(url, 'OCCUPATIONGROUPsBYSTATE')
    
    # convert the tables to pandas dataframe
    occu_grpDF = occu_grp.toPandas()
    stateareasDF = stateareas.toPandas()

    # rename id value to AREAID in stateareaDF
    stateareasDF.columns.values[3] = 'AREAID'

    # merge the occupation group dataframe and statearea dataframe on areaID
    occGrpData = occu_grpDF.merge(stateareasDF, on='AREAID')

    # create subset
    occu_data_col = ['stateName', 'stateAreaName', 'NAME', 'SALARYTICKERREALTIME', 'SALARY25TH', 'SALARY75TH', 'SALARYAVERAGE', 'SALARYREALTIME25TH', 'SALARYREALTIME75TH', 'SALARYREALTIMEAVERAGE']
    occGrpSal = occGrpData[occu_data_col]
    
    # Split stateAreaName column into STATECODE and STATEAREA
    func = lambda x: pd.Series([i for i in reversed(x.split(','))])
    stateDF = occGrpSal['stateAreaName'].apply(func)
    stateDF.rename(columns={0:'STATECODE',1:'STATEAREA'},inplace=True)

    # merge the newly split column dataframe to occGrpSal
    occGrpSal = pd.concat([occGrpSal, stateDF], axis=1)
    
    # drop the rows where STATECODE = 'All States' & STATECODE = 'All Areas'
    occGrpSal = occGrpSal.drop(occGrpSal[occGrpSal.STATECODE == 'All States'].index)
    occGrpSal = occGrpSal.drop(occGrpSal[occGrpSal.STATECODE == ' All Areas'].index)
    
    return occGrpSal


# In[5]:

def get_States_Sal(states_dict):
    # call get_Data() function to get salary data
    salStateData = get_data()
    
    # extract states lat and lon information for generating map
    states = {code: state for code, state in states_dict.items()}
    
    # sort the data by state names
    states_Name = sorted(states.values(), key = lambda x : x['name'])

    state_xs = [state["lons"] for state in states_Name]
    state_ys = [state["lats"] for state in states_Name]
    state_names = [state["name"] for state in states_Name]

    # create column data source
    source = ColumnDataSource(data=dict(x = state_xs, y = state_ys,
        stateN = [name for name in state_names]))
    
    # get average of salary occupation group in each state
    salStateAgg = pd.DataFrame(salStateData.groupby(['STATECODE', 'stateName'], axis=0, as_index=False)['SALARYAVERAGE', 'SALARYREALTIMEAVERAGE'].mean()).reset_index()
    
    # Create colorMap dictionary
    keys = tuple(pd.unique(salStateAgg["SALARYREALTIMEAVERAGE"]))
    values = tuple(["#000000", "#FFFF00", "#1CE6FF", "#FF34FF", "#FF4A46", "#008941", "#006FA6", "#A30059",
        "#FFDBE5", "#7A4900", "#0000A6", "#63FFAC", "#B79762", "#004D43", "#8FB0FF", "#997D87",
        "#5A0007", "#809693", "#FEFFE6", "#1B4400", "#4FC601", "#3B5DFF", "#4A3B53", "#FF2F80",
        "#61615A", "#BA0900", "#6B7900", "#00C2A0", "#FFAA92", "#FF90C9", "#B903AA", "#D16100",                
        "#DDEFFF", "#000035", "#7B4F4B", "#A1C299", "#300018", "#0AA6D8", "#013349", "#00846F",
        "#372101", "#FFB500", "#C2FFED", "#A079BF", "#CC0744", "#C0B9B2", "#C2FF99", "#001E09",
        "#00489C", "#6F0062", "#0CBD66", "#EEC3FF"])

    colorMap = dict(itertools.izip(keys, values))
    
    # add values to the source
    source.add(data = [str(x) for x in salStateAgg["STATECODE"]], name = 'statecode')
    source.add(data = [str(x) for x in salStateAgg["SALARYAVERAGE"]], name = 'salAvg')
    source.add(data = [str(x) for x in salStateAgg["SALARYREALTIMEAVERAGE"]], name = 'salRealAvg')
    source.add(data = [colorMap[x] for x in salStateAgg["SALARYREALTIMEAVERAGE"]], name = 'type_color')

    return source


# In[6]:

def get_OccGrp_Data():
    salOccData = get_data()
    
    # get average of salary occupation group in each state
    occGrpDataAgg = pd.DataFrame(salOccData.groupby(['NAME', 'STATECODE', 'stateName'], axis=0, as_index=False)['SALARYAVERAGE', 'SALARYREALTIMEAVERAGE'].mean()).reset_index()
    
    # get top 10 salaries in occupation group and in each state
    occGrpStateTop10 = (occGrpDataAgg.assign(rn=occGrpDataAgg.sort_values(['SALARYREALTIMEAVERAGE', 'SALARYAVERAGE'], ascending=False).groupby(['stateName']).cumcount() + 1).query('rn <= 10').sort_values(['stateName','rn']))
    
    # Create colorMap dictionary
    keys = tuple(pd.unique(occGrpStateTop10.NAME))
    values = tuple(["#000000", "#FFFF00", "#1CE6FF", "#FF34FF", "#FF4A46", "#008941", "#006FA6", "#A30059",
        "#FFDBE5", "#7A4900", "#0000A6", "#63FFAC", "#B79762", "#004D43", "#8FB0FF", "#997D87",
        "#5A0007", "#809693", "#FEFFE6", "#1B4400", "#4FC601", "#3B5DFF", "#4A3B53", "#FF2F80",
        "#61615A", "#BA0900", "#6B7900", "#00C2A0", "#FFAA92", "#FF90C9", "#B903AA", "#D16100",                
        "#DDEFFF", "#000035", "#7B4F4B", "#A1C299", "#300018"]) 
    
    colorMap = dict(itertools.izip(keys, values))
    
    # extract distinct statenames and ranks and convert numeric data to string
    stateName = [str(x) for x in list(pd.unique(occGrpStateTop10.stateName))]
    ranks = [str(x) for x in sorted(list(pd.unique(occGrpStateTop10.rn)))]
    occGrpStateTop10["SALARYREALTIMEAVERAGE"] = occGrpStateTop10["SALARYREALTIMEAVERAGE"].astype(str)
    occGrpStateTop10["SALARYAVERAGE"] = occGrpStateTop10["SALARYAVERAGE"].astype(str)
    
    # create data dictionary for visualization
    source = ColumnDataSource(
    data=dict(
        ar=[str(x) for x in occGrpStateTop10["rn"]],
        code=[str(y) for y in occGrpStateTop10["stateName"]],
        symx=[str(x)+":0.1" for x in occGrpStateTop10["rn"]],
        rsa=[str(x)+":0.8" for x in occGrpStateTop10["rn"]],
        sa=[str(x)+":0.15" for x in occGrpStateTop10["rn"]],
        namey=[str(x)+":0.3" for x in occGrpStateTop10["rn"]],
        name=occGrpStateTop10["NAME"],
        realSalAvg=occGrpStateTop10["SALARYREALTIMEAVERAGE"],
        salAvg=occGrpStateTop10["SALARYAVERAGE"],
        type_color=[colorMap[x] for x in occGrpStateTop10["NAME"]],
        )
    )
    return source, ranks, stateName


# In[7]:

# get column data source dictionary
sourceState = get_States_Sal(states_dict)
sourceOccGrp, ranks, stateNm = get_OccGrp_Data()

# save Tool operation in a variable
TOOLS = "pan, wheel_zoom, box_zoom, reset, save, tap"


# In[8]:

# pass parameters to generate the map
labels = LabelSet(x= 'x', y='y', text='statecode', level='glyph', 
                  x_offset=0, y_offset=0, source=sourceState, text_font_size='3pt',
                  render_mode = 'canvas')

mp = figure(tools=TOOLS, toolbar_location="above",
    x_axis_location=None, y_axis_location=None, 
    plot_width=1100, plot_height=800, x_range=(-170,-65), y_range=(7,75)
)
mp.grid.grid_line_color = None
mp.title.text_font_size = '20pt'

#print world map as background
mp1 = mp.patches(
    'x', 'y', source=get_geo_world(),
    fill_color="#F1EEF6", fill_alpha=0.7, line_width=0.5)

# add the salary data to the map
mp2 = mp.patches("x", "y", source=sourceState,
    color = "type_color", 
    fill_alpha=0.7, line_color="grey", line_width=0.5)

# add lables to map
mp.add_layout(labels)

# add hover text
mp.add_tools(HoverTool(renderers=[mp2],
    point_policy = "follow_mouse",
    tooltips = [
    ("State", "@stateN"),
        ("Salary Average", "$"+"@salAvg"),
        ("Salary RealTime Average", "$"+"@salRealAvg"),
        ("(Lon, Lat)", "($x, $y)"),]))

tab1 = Panel(child=mp, title="Salary Average Distribution")


# In[9]:

# pass parameters for figure 1
p = figure(tools=TOOLS, toolbar_location="above",
           x_range=ranks, y_range=list(reversed(stateNm)),
          plot_width=3000, plot_height=2000)

p.outline_line_color = None

p.rect("ar", "code", .97, .97, source=sourceOccGrp,
       fill_alpha=0.5, color="type_color")

text_props = {
    "source": sourceOccGrp,
    "angle": 0,
    "color": "black",
    "text_align": "left",
    "text_baseline": "middle"
}

p.text(x="symx", y="code", text="name",
       text_font_style="bold", text_font_size="9pt", **text_props)

p.grid.grid_line_color = None

p.add_tools(HoverTool(
        point_policy = "follow_mouse",
        tooltips = [
            ("name", "@name"),
            ("Salary Realtime Average", "@realSalAvg"),
            ("Salary Average", "@salAvg"),
            ("State", "@code"),]))

# create a panel for tab1
tab2 = Panel(child=p, title="Top 10 Salary Average and Salary Realtime Avegare by Occupation Group - State")


# In[10]:

# combine the tabs
tabs = Tabs(tabs=[tab1, tab2])


# In[11]:

#Display data
output_notebook()

show(tabs)


# In[12]:

get_ipython().run_cell_magic(u'javascript', u'', u"require.config({\n  paths: {\n      d3: '//cdnjs.cloudflare.com/ajax/libs/d3/3.4.8/d3.min'\n  }\n});")


# In[ ]:



