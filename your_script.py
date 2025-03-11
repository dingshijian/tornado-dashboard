from flask import Flask, jsonify

# Add a new Flask route to serve tornado data as JSON
server = app.server  # Use Dash's Flask instance

@server.route('/tornado-data')
def get_tornado_data():
    """Serve tornado data as JSON for the HTML page."""
    tornado_json = df_grouped.to_dict(orient="records")
    return jsonify(tornado_json)