from flask import Flask, request

app = Flask(__name__)

# https://accounts.spotify.com/authorize?client_id=4f2cf04877ec4412a3e5426ae11c4256&response_type=code&redirect_uri=http://127.0.0.1:8888/callback&scope=user-read-recently-played&state=10420520402502815


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    received_state = request.args.get('state')
    return f'Authorization code: {auth_code}\nstate:{received_state}'

if __name__ == "__main__":
    app.run(port=8888)