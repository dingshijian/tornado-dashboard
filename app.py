import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd

# -----------------------------
# Load and Prepare Data
# -----------------------------
data_path = '/Users/dings/Downloads/us-weather-events-1980-2024.csv'

# Read the CSV with low_memory=False to avoid DtypeWarning
df_raw = pd.read_csv(data_path, low_memory=False)

# Filter for tornado events.
# Adjust the filtering if your EVENT_TYPE values differ.
df_tornado = df_raw[df_raw['EVENT_TYPE'].str.contains("Tornado", case=False, na=False)].copy()

# Convert the "BEGIN_DATE_TIME" column to datetime.
# If you know the exact format (for example, '%m/%d/%Y %H:%M:%S'), you can specify it here.
df_tornado['BEGIN_DATE_TIME'] = pd.to_datetime(
    df_tornado['BEGIN_DATE_TIME'],
    errors='coerce'
)

# Drop rows with invalid dates and extract the year.
df_tornado = df_tornado.dropna(subset=['BEGIN_DATE_TIME'])
df_tornado['year'] = df_tornado['BEGIN_DATE_TIME'].dt.year

# Group by state and year to count the number of tornado events.
df_grouped = df_tornado.groupby(['STATE', 'year']).size().reset_index(name='tornado_count')

# --- Debug: Print unique years and states in the grouped data ---
unique_years = sorted(df_grouped['year'].unique())
print("Years in the data:", unique_years)

# Standardize state names to title case
df_grouped['STATE'] = df_grouped['STATE'].str.title()


# Function to get state abbreviations
def get_state_abbrev(state_name):
    state_abbrev_map = {
        'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
        'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
        'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
        'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
        'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
        'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
        'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
        'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
        'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
        'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
        'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
        'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
        'Wisconsin': 'WI', 'Wyoming': 'WY'
    }
    return state_abbrev_map.get(state_name, None)  # Return None if the state is not found

# Now apply the function correctly
df_grouped['state_abbrev'] = df_grouped['STATE'].apply(get_state_abbrev)

# Drop any rows where a valid state abbreviation could not be determined
df_grouped = df_grouped.dropna(subset=['state_abbrev'])

def get_state_abbrev(state_val):
    """Return the two-letter abbreviation for a given state value.
       If the value is already two letters, assume it is valid.
       Otherwise, convert to title case and look up in state_abbrev_map.
       Returns None if no valid abbreviation can be determined."""
    s = str(state_val).strip()
    if len(s) == 2:
        return s.upper()
    # Convert to title case (e.g., "ALABAMA" -> "Alabama")
    s_title = s.title()
    return state_abbrev_map.get(s_title, None)

# Apply the helper function to create a new column for state abbreviations.
df_grouped['state_abbrev'] = df_grouped['STATE'].apply(get_state_abbrev)

# Drop any rows where a valid state abbreviation could not be determined.
df_grouped = df_grouped.dropna(subset=['state_abbrev'])

# Create an inverse mapping (abbreviation -> full state name) for use in labels.
abbrev_state = {}
for s in df_grouped['STATE'].unique():
    abbr = get_state_abbrev(s)
    if abbr is not None:
        # Use the title-cased full name for clarity.
        abbrev_state[abbr] = s.title()

# Determine the list of available years for the dropdown.
available_years = sorted(df_grouped['year'].unique())
print("Available years for dropdown:", available_years)

# -----------------------------
# Dash App Setup
# -----------------------------
app = dash.Dash(__name__)
app.title = "US Tornado Dashboard"

app.layout = html.Div([
    html.H1("US Tornado Dashboard", style={'textAlign': 'center'}),
    
    # Year selection dropdown
    html.Div([
        html.Label("Select Year:"),
        dcc.Dropdown(
            id='year-dropdown',
            options=[{'label': str(year), 'value': year} for year in available_years],
            value=available_years[0] if available_years else None,
            clearable=False,
            style={'width': '150px', 'margin': '0 auto'}
        )
    ], style={'textAlign': 'center', 'padding': '10px'}),
    
    # Choropleth Map
    dcc.Graph(id='choropleth-map'),
    
    html.Hr(),
    
    # Line chart for tornado trend of clicked state.
    html.Div([
        html.H2(id='line-chart-title', style={'textAlign': 'center'}),
        dcc.Graph(id='line-chart')
    ])
])

# -----------------------------
# Callbacks
# -----------------------------
# 1. Update the choropleth map based on the selected year.
@app.callback(
    Output('choropleth-map', 'figure'),
    Input('year-dropdown', 'value')
)
def update_choropleth(selected_year):
    if selected_year is None:
        return {}
    # Filter the data for the selected year.
    dff = df_grouped[df_grouped['year'] == selected_year]
    
    # Create the choropleth map.
    fig = px.choropleth(
        dff,
        locations='state_abbrev',
        locationmode="USA-states",
        color='tornado_count',
        color_continuous_scale="Reds",
        scope="usa",
        labels={'tornado_count': 'Tornado Count'},
        hover_data={'STATE': True, 'tornado_count': True, 'year': False}
    )
    fig.update_layout(
        title_text=f"Tornado Count by State in {selected_year}",
        margin={"r": 0, "t": 30, "l": 0, "b": 0}
    )
    return fig

# 2. Update the line chart when a state is clicked on the map.
@app.callback(
    [Output('line-chart', 'figure'),
     Output('line-chart-title', 'children')],
    Input('choropleth-map', 'clickData')
)
def update_line_chart(clickData):
    # If no state is clicked, display a message.
    if clickData is None:
        return {}, "Click on a state in the map to see the tornado trend over the years."
    
    # Extract the state abbreviation from the clickData.
    state_abbrev_clicked = clickData['points'][0]['location']
    # Get the full state name (or use the abbreviation if that's all we have).
    state_full = abbrev_state.get(state_abbrev_clicked, state_abbrev_clicked)
    
    # Filter data for the selected state across all years.
    dff = df_grouped[df_grouped['state_abbrev'] == state_abbrev_clicked]
    
    if dff.empty:
        return {}, f"No tornado data found for {state_full}."
    
    # Create the line chart.
    fig = px.line(dff, x='year', y='tornado_count',
                  markers=True,
                  title=f"Tornado Trend in {state_full}")
    fig.update_layout(xaxis=dict(dtick=1))
    
    title = f"Tornado Counts Over the Years for {state_full}"
    return fig, title

# -----------------------------
# Run the App
# -----------------------------
if __name__ == '__main__':
    app.run_server(debug=True)
