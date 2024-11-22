
import socket
from flask import Flask, jsonify, render_template
import random
from threading import Timer

app = Flask(__name__)

def generate_data():
    try:
        # Define the server address and port
        server_address = ('127.0.0.1', 5555)  # Replace with your server's address and port

        # Create a TCP socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the server
            sock.connect(server_address)

            # Send the "ring:" command
            sock.sendall(b'ring:')

            # Wait for the reply
            data = sock.recv(1024).decode('utf-8')  # Receive up to 1024 bytes

            # Parse the response
            if data.startswith('ring_ack:'):
                # Extract and return the data portion
                return eval(data[len('ring_ack:'):])  # Convert string to dictionary
            else:
                print("Unexpected response:", data)
                return {}
    except Exception as e:
        print("Error during TCP communication:", e)
        return {}

# Store initial data
current_data = generate_data()

# Simulate data updates
def update_data():
    global current_data
    current_data = generate_data()
    Timer(1.0, update_data).start()  # Schedule the next update

update_data()  # Start data updates

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def data():
    return jsonify(current_data)

if __name__ == "__main__":
    app.run(debug=True)

