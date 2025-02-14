from flask import Flask
import os
from trinity import Trinity
from db_quieries import StoragePipeline

app = Flask(__name__)

@app.route('/') # route called by user
def index(): # function called by '/' route
    return 'Hello Trinity! This App is built using Flask.'

@app.route('/trinity')
def trinity_batch():
    run_trinity = Trinity().find_target_region()
    return'ran trinity: batch mode'





if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))