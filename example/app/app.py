import os
from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    node_id = os.environ.get('NODE_ID', 'Unknown')
    return f'Hello from {node_id}'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)