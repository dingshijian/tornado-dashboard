import os
import pandas as pd
import requests
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go

# 🔹 Google Drive File ID (Extracted from your link)
file_id = "1WDsm4qBNcGg8MOskRcLvSRGRU41Ef6rX"

# 🔹 Construct direct Google Drive download URL with confirmation parameter
csv_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"

# 🔹 Define the local file path
csv_path = os.path.join(os.path.dirname(__file__), 'us-weather-events-1980-2024.csv')

# 🔹 Download the file if it does not exist
if not os.path.exists(csv_path):
    print("📥 Downloading CSV file from Google Drive...")
    try:
        response = requests.get(csv_url, stream=True, timeout=30)
        # Check if the response is HTML (indicating a virus scan warning page)
        if "text/html" in response.headers.get("Content-Type", ""):
            print("❌ Detected Google Drive virus scan warning. Verify the URL and file permissions.")
            exit(1)
        if response.status_code == 200:
            with open(csv_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print("✅ Download complete.")
            # Verify CSV integrity by attempting to read just the header
            try:
                pd.read_csv(csv_path, nrows=0)
            except Exception as e:
                print("❌ Invalid CSV format. Deleting corrupted file.")
                os.remove(csv_path)
                exit(1)
        else:
            print(f"❌ Failed to download CSV. Status code: {response.status_code}")
            exit(1)
    except requests.RequestException as e:
        print(f"❌ Error downloading file: {e}")
        if os.path.exists(csv_path):
            os.remove(csv_path)
        exit(1)

# ✅ Process the CSV using chunked reading to reduce memory usage
print("📌 Processing CSV file in chunks...")
try:
    # Load necessary columns including coordinates and EF scale
    use_columns = ["EVENT_TYPE", "BEGIN_DATE_TIME", "STATE", "BEGIN_LAT", "BEGIN_LON", "TOR_F_SCALE"]
    chunks = []
    chunk_size = 100000  # Adjust chunk size as needed

    for chunk in pd.read_csv(csv_path, encoding="utf-8", low_memory=False, usecols=use_columns, chunksize=chunk_size):
        # Standardize column names
        chunk.columns = chunk.columns.str.strip().str.upper()
        # Filter for tornado events
        tornado_chunk = chunk[chunk["EVENT_TYPE"].str.contains("Tornado", case=False, na=False)].copy()
        chunks.append(tornado_chunk)

    df_tornado = pd.concat(chunks, ignore_index=True)

    # Convert the "BEGIN_DATE_TIME" column to datetime
    df_tornado["BEGIN_DATE_TIME"] = pd.to_datetime(df_tornado["BEGIN_DATE_TIME"], errors="coerce")
    
    # Drop rows with invalid dates and extract the year
    df_tornado = df_tornado.dropna(subset=["BEGIN_DATE_TIME"])
    df_tornado["YEAR"] = df_tornado["BEGIN_DATE_TIME"].dt.year
    
    # Drop rows with missing coordinates
    df_tornado = df_tornado.dropna(subset=["BEGIN_LAT", "BEGIN_LON"])
    
    # Clean and standardize EF scale
    df_tornado["TOR_F_SCALE"] = df_tornado["TOR_F_SCALE"].str.upper().str.strip()
    # Only keep valid EF/F scales
    valid_scales = ["F0", "F1", "F2", "F3", "F4", "F5", "EF0", "EF1", "EF2", "EF3", "EF4", "EF5"]
    df_tornado = df_tornado[df_tornado["TOR_F_SCALE"].isin(valid_scales)]
    
    # For visualization purposes, convert older F scale to EF equivalent
    df_tornado["EF_SCALE"] = df_tornado["TOR_F_SCALE"].str.replace("F", "EF")
    
    # Create a numeric scale for severity coloring
    ef_to_num = {"EF0": 0, "EF1": 1, "EF2": 2, "EF3": 3, "EF4": 4, "EF5": 5}
    df_tornado["SEVERITY"] = df_tornado["EF_SCALE"].map(ef_to_num)

    # Group by state and year to count tornadoes
    df_grouped = df_tornado.groupby(["STATE", "YEAR"]).size().reset_index(name="TORNADO_COUNT")

    # Group by state, year, and EF scale for detailed analysis
    df_ef_grouped = df_tornado.groupby(["STATE", "YEAR", "EF_SCALE"]).size().reset_index(name="COUNT")

    print("📌 Columns Found in CSV:")
    print(df_tornado.columns.tolist())
    print("📌 First 5 Rows:")
    print(df_tornado.head())
except Exception as e:
    print(f"❌ Error processing CSV: {e}")
    exit(1)

# Standardize state names to title case
df_grouped["STATE"] = df_grouped["STATE"].str.title()
df_ef_grouped["STATE"] = df_ef_grouped["STATE"].str.title()

# 🔹 Function to get state abbreviations
state_abbrev_map = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY"
}

def get_state_abbrev(state_name):
    return state_abbrev_map.get(state_name, None)

# Apply the function to add a state abbreviation column
df_grouped["STATE_ABBREV"] = df_grouped["STATE"].apply(get_state_abbrev)
df_grouped = df_grouped.dropna(subset=["STATE_ABBREV"])

df_ef_grouped["STATE_ABBREV"] = df_ef_grouped["STATE"].apply(get_state_abbrev)
df_ef_grouped = df_ef_grouped.dropna(subset=["STATE_ABBREV"])

# Create an inverse mapping (abbreviation -> full state name)
abbrev_state = {abbr: state for state, abbr in state_abbrev_map.items()}

# Determine available years for the dropdown
available_years = sorted(df_tornado["YEAR"].unique())

# Define EF scale colors
ef_colors = {
    "EF0": "#abd9e9",  # Light blue
    "EF1": "#74add1",  # Blue
    "EF2": "#4575b4",  # Dark blue
    "EF3": "#f46d43",  # Orange
    "EF4": "#d73027",  # Red
    "EF5": "#a50026"   # Dark red/purple
}

# -----------------------------
# Dash App Setup
# -----------------------------
app = dash.Dash(__name__)
app.title = "US Tornado Dashboard"

# Expose the Flask server for Gunicorn
server = app.server

app.layout = html.Div([
    html.H1("US Tornado Dashboard (1980-2024)", style={"textAlign": "center"}),
    
    html.Div([
        html.Div([
            html.Label("Select Year:"),
            dcc.Dropdown(
                id="year-dropdown",
                options=[{"label": str(year), "value": year} for year in available_years],
                value=available_years[-1] if available_years else None,  # Default to most recent year
                clearable=False,
                style={"width": "100%"}
            )
        ], style={"width": "30%", "display": "inline-block", "padding": "10px"}),
        
        html.Div([
            html.Label("EF Scale Filter:"),
            dcc.Checklist(
                id="ef-scale-filter",
                options=[
                    {"label": f"EF0", "value": "EF0"},
                    {"label": f"EF1", "value": "EF1"},
                    {"label": f"EF2", "value": "EF2"},
                    {"label": f"EF3", "value": "EF3"},
                    {"label": f"EF4", "value": "EF4"},
                    {"label": f"EF5", "value": "EF5"}
                ],
                value=["EF0", "EF1", "EF2", "EF3", "EF4", "EF5"],  # All selected by default
                inline=True
            )
        ], style={"width": "70%", "display": "inline-block", "padding": "10px"})
    ]),
    
    # Tab navigation
    dcc.Tabs([
        dcc.Tab(label="Tornado Map", children=[
            html.Div([
                dcc.Graph(id="tornado-map")
            ], style={"padding": "20px"})
        ]),
        
        dcc.Tab(label="State Heatmap", children=[
            html.Div([
                dcc.Graph(id="choropleth-map")
            ], style={"padding": "20px"})
        ])
    ]),
    
    html.Hr(),
    
    html.Div([
        html.Div([
            html.H2(id="line-chart-title", style={"textAlign": "center"}),
            dcc.Graph(id="line-chart")
        ], style={"width": "50%", "display": "inline-block"}),
        
        html.Div([
            html.H2(id="ef-distribution-title", style={"textAlign": "center"}),
            dcc.Graph(id="ef-distribution-chart")
        ], style={"width": "50%", "display": "inline-block"})
    ])
])

# -----------------------------
# Callbacks
# -----------------------------
@app.callback(
    Output("tornado-map", "figure"),
    [Input("year-dropdown", "value"),
     Input("ef-scale-filter", "value")]
)
def update_tornado_map(selected_year, selected_ef_scales):
    if selected_year is None:
        return {}
    
    # Filter data for selected year and EF scales
    dff = df_tornado[(df_tornado["YEAR"] == selected_year) & 
                     (df_tornado["EF_SCALE"].isin(selected_ef_scales))]
    
    # Create scatter mapbox for tornado locations
    fig = px.scatter_mapbox(
        dff,
        lat="BEGIN_LAT",
        lon="BEGIN_LON",
        color="EF_SCALE",
        color_discrete_map=ef_colors,
        hover_name="STATE",
        hover_data={
            "EF_SCALE": True,
            "BEGIN_DATE_TIME": True,
            "BEGIN_LAT": False,
            "BEGIN_LON": False
        },
        zoom=3,
        height=600
    )
    
    fig.update_layout(
        title=f"Tornado Events in {selected_year} by EF Scale",
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 50, "l": 0, "b": 0}
    )
    
    return fig

@app.callback(
    Output("choropleth-map", "figure"),
    [Input("year-dropdown", "value"),
     Input("ef-scale-filter", "value")]
)
def update_choropleth(selected_year, selected_ef_scales):
    if selected_year is None:
        return {}
    
    # Filter data for the selected year
    dff_ef = df_ef_grouped[(df_ef_grouped["YEAR"] == selected_year) & 
                           (df_ef_grouped["EF_SCALE"].isin(selected_ef_scales))]
    
    # Aggregate counts for the selected EF scales
    dff = dff_ef.groupby(["STATE", "STATE_ABBREV"]).sum().reset_index()
    
    fig = px.choropleth(
        dff,
        locations="STATE_ABBREV",
        locationmode="USA-states",
        color="COUNT",
        color_continuous_scale="Reds",
        scope="usa",
        labels={"COUNT": "Tornado Count"},
        hover_data={"STATE": True, "COUNT": True}
    )
    
    fig.update_layout(
        title_text=f"Tornado Count by State in {selected_year}",
        height=600
    )
    
    return fig

@app.callback(
    [Output("line-chart", "figure"),
     Output("line-chart-title", "children"),
     Output("ef-distribution-chart", "figure"),
     Output("ef-distribution-title", "children")],
    [Input("choropleth-map", "clickData"),
     Input("ef-scale-filter", "value")]
)
def update_charts(clickData, selected_ef_scales):
    if clickData is None:
        # Default view: show national trends
        state_abbrev_clicked = None
        state_full = "United States"
    else:
        state_abbrev_clicked = clickData["points"][0]["location"]
        state_full = abbrev_state.get(state_abbrev_clicked, state_abbrev_clicked)
    
    # For line chart
    if state_abbrev_clicked:
        # Filter for specific state
        dff_ef = df_ef_grouped[(df_ef_grouped["STATE_ABBREV"] == state_abbrev_clicked) & 
                              (df_ef_grouped["EF_SCALE"].isin(selected_ef_scales))]
    else:
        # National data
        dff_ef = df_ef_grouped[df_ef_grouped["EF_SCALE"].isin(selected_ef_scales)]
    
    # Group by year across all selected EF scales
    dff_yearly = dff_ef.groupby("YEAR")["COUNT"].sum().reset_index()
    
    line_fig = px.line(dff_yearly, x="YEAR", y="COUNT", markers=True)
    line_fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Tornado Count"
    )
    
    # For EF distribution chart
    if state_abbrev_clicked:
        # State-specific
        dff_ef_dist = df_ef_grouped[df_ef_grouped["STATE_ABBREV"] == state_abbrev_clicked]
    else:
        # National
        dff_ef_dist = df_ef_grouped.copy()
    
    # Group by EF scale
    dff_ef_summary = dff_ef_dist.groupby("EF_SCALE")["COUNT"].sum().reset_index()
    
    # Order by EF scale severity
    ef_order = ["EF0", "EF1", "EF2", "EF3", "EF4", "EF5"]
    dff_ef_summary["EF_SCALE"] = pd.Categorical(dff_ef_summary["EF_SCALE"], categories=ef_order, ordered=True)
    dff_ef_summary = dff_ef_summary.sort_values("EF_SCALE")
    
    ef_dist_fig = px.bar(
        dff_ef_summary, 
        x="EF_SCALE", 
        y="COUNT",
        color="EF_SCALE",
        color_discrete_map=ef_colors
    )
    
    ef_dist_fig.update_layout(
        xaxis_title="EF Scale",
        yaxis_title="Total Count (1980-2024)"
    )
    
    line_title = f"Tornado Trend in {state_full} (1980-2024)"
    dist_title = f"EF Scale Distribution in {state_full} (1980-2024)"
    
    return line_fig, line_title, ef_dist_fig, dist_title

# -----------------------------
# Run the App (for local development)
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(debug=True, host="0.0.0.0", port=port)