from flask import Flask
from node import Cluster

app = Flask(__name__)

if __name__ == "__main__":
    Cluster()    
    app.run(host ='0.0.0.0', port = 5000, debug = True)
