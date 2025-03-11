import os
import pandas as pd
import requests
import dash
from dash import dcc, html, Input, Output
import plotly.express as px

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
        # Check if response is HTML (indicating a virus scan warning page)
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
            os.remove(csv_path)  # Remove partial download
        exit(1)

# ✅ Process the CSV using chunked reading to reduce memory usage
print("📌 Processing CSV file in chunks...")

try:
    # Only load necessary columns to reduce memory usage.
    # Adjust these columns based on your CSV structure.
    use_columns = ["EVENT_TYPE", "BEGIN_DATE_TIME", "STATE"]
    chunks = []
    chunk_size = 100000  # Adjust chunk size if needed

    for chunk in pd.read_csv(csv_path, encoding="utf-8", low_memory=False, usecols=use_columns, chunksize=chunk_size):
        # Standardize column names
        chunk.columns = chunk.columns.str.strip().str.upper()
        # Filter for tornado events
        tornado_chunk = chunk[chunk["EVENT_TYPE"].str.contains("Tornado", case=False, na=False)].copy()
        chunks.append(tornado_chunk)
    
    df_tornado = pd.concat(chunks, ignore_index=True)

    # Convert the "BEGIN_DATE_TIME" column to datetime.
    df_tornado["BEGIN_DATE_TIME"] = pd.to_datetime(df_tornado["BEGIN_DATE_TIME"], errors="coerce")
    # Drop rows with invalid dates and extract the year.
    df_tornado = df_tornado.dropna(subset=["BEGIN_DATE_TIME"])
    df_tornado["year"] = df_tornado["BEGIN_DATE_TIME"].dt.year

    # Group by state and year to count tornadoes.
    df_grouped = df_tornado.groupby(["STATE", "year"]).size().reset_index(name="tornado_count")

    # Debugging output
    print("📌 Columns Found in CSV:")
    print(df_tornado.columns.tolist())
    print("📌 First 5 Rows:")
    print(df_tornado.head())

except Exception as e:
    print(f"❌ Error processing CSV: {e}")
    exit(1)

# Standardize state names to title case
df_grouped["STATE"] = df_grouped["STATE"].str.title()

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
df_grouped["state_abbrev"] = df_grouped["STATE"].apply(get_state_abbrev)
df_grouped = df_grouped.dropna(subset=["state_abbrev"])

# Create an inverse mapping (abbreviation -> full state name)
abbrev_state = {abbr: state for state, abbr in state_abbrev_map.items()}

# Determine available years for the dropdown
available_years = sorted(df_grouped["year"].unique())

# -----------------------------
# Dash App Setup
# -----------------------------
app = dash.Dash(__name__)
app.title = "US Tornado Dashboard"

app.layout = html.Div([
    html.H1("US Tornado Dashboard", style={"textAlign": "center"}),
    
    # Year selection dropdown
    html.Div([
        html.Label("Select Year:"),
        dcc.Dropdown(
            id="year-dropdown",
            options=[{"label": str(year), "value": year} for year in available_years],
            value=available_years[0] if available_years else None,
            clearable=False,
            style={"width": "150px", "margin": "0 auto"}
        )
    ], style={"textAlign": "center", "padding": "10px"}),
    
    # Choropleth Map
    dcc.Graph(id="choropleth-map"),
    
    html.Hr(),
    
    # Line chart for tornado trends
    html.Div([
        html.H2(id="line-chart-title", style={"textAlign": "center"}),
        dcc.Graph(id="line-chart")
    ])
])

# -----------------------------
# Callbacks
# -----------------------------
@app.callback(
    Output("choropleth-map", "figure"),
    Input("year-dropdown", "value")
)
def update_choropleth(selected_year):
    if selected_year is None:
        return {}
    
    dff = df_grouped[df_grouped["year"] == selected_year]
    
    fig = px.choropleth(
        dff,
        locations="state_abbrev",
        locationmode="USA-states",
        color="tornado_count",
        color_continuous_scale="Reds",
        scope="usa",
        labels={"tornado_count": "Tornado Count"},
        hover_data={"STATE": True, "tornado_count": True, "year": False}
    )
    fig.update_layout(title_text=f"Tornado Count by State in {selected_year}")
    return fig

@app.callback(
    [Output("line-chart", "figure"),
     Output("line-chart-title", "children")],
    Input("choropleth-map", "clickData")
)
def update_line_chart(clickData):
    if clickData is None:
        return {}, "Click a state to see its tornado trend."
    
    state_abbrev_clicked = clickData["points"][0]["location"]
    state_full = abbrev_state.get(state_abbrev_clicked, state_abbrev_clicked)
    
    dff = df_grouped[df_grouped["state_abbrev"] == state_abbrev_clicked]
    
    if dff.empty:
        return {}, f"No tornado data found for {state_full}."
    
    fig = px.line(dff, x="year", y="tornado_count", markers=True)
    title = f"Tornado Counts in {state_full} Over the Years"
    
    return fig, title

# -----------------------------
# Run the App
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run_server(debug=True, host="0.0.0.0", port=port)