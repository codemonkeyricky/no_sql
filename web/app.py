
from flask import Flask, jsonify, render_template
import random
from threading import Timer

app = Flask(__name__)

# Generate dummy data
def generate_data():
    return {
        f"600{random.randint(0, 9)}-0": random.randint(1000000, 50000000),
        f"555{random.randint(5, 6)}-0": random.randint(1000000, 50000000),
        f"601{random.randint(0, 9)}-0": random.randint(1000000, 50000000),
    }

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

