# Flask app which is needed to get the authorization code initially 

from flask import Flask, request

app = Flask(__name__)


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    return f'Authorization code: {auth_code}'

if __name__ == "__main__":
    app.run(port=8888)