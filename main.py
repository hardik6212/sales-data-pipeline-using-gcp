from flask import Flask, render_template, request, redirect, url_for
from google.cloud import storage
import os

app = Flask(__name__)

# Initialize the Google Cloud Storage client
def upload_to_gcs(file, bucket_name):
    """Uploads a file to the given GCS bucket."""
    # Create a storage client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file.filename)
    
    # Upload file
    blob.upload_from_file(file)
    return f"File {file.filename} uploaded to {bucket_name}."

# The GCS bucket name
BUCKET_NAME = 'salesdatapipeline_bucket'

@app.route('/')
def index():
    return render_template('index.html',content="null")

@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        # Get the file from the form
        uploaded_file = request.files['file']
        if uploaded_file.filename != '':
            # Call the GCS upload function
            message = upload_to_gcs(uploaded_file, BUCKET_NAME)
            return message
        return "No file uploaded"
    
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
