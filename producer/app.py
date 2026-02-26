from flask import Flask, request, jsonify, send_from_directory
import json
import os
from datetime import datetime

app = Flask(__name__, static_folder='static')

ORDERS_DIR = os.path.join(os.path.dirname(__file__), 'orders')
os.makedirs(ORDERS_DIR, exist_ok=True)


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/orders', methods=['POST'])
def create_order():
    data = request.get_json()

    # Build order object matching the schema
    order = {
        "order_id": data['order_id'],
        "order_date": data['order_date'],
        "total_amount": round(sum(
            p['price'] * p['quantity'] for p in data['products']
        ), 2),
        "customer": {
            "customer_id": data['customer']['customer_id'],
            "name": data['customer']['name'],
            "email": data['customer']['email'],
            "address": data['customer']['address']
        },
        "products": [
            {
                "product_id": p['product_id'],
                "name": p['name'],
                "category": p['category'],
                "price": p['price'],
                "quantity": p['quantity']
            }
            for p in data['products']
        ]
    }

    filename = f"order_{order['order_id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    filepath = os.path.join(ORDERS_DIR, filename)

    with open(filepath, 'w') as f:
        json.dump(order, f, indent=2)

    return jsonify({
        "success": True,
        "message": f"Order saved as {filename}",
        "order": order,
        "filepath": filepath
    }), 201


@app.route('/api/orders', methods=['GET'])
def list_orders():
    files = sorted(os.listdir(ORDERS_DIR), reverse=True)
    orders = []
    for f in files:
        if f.endswith('.json'):
            with open(os.path.join(ORDERS_DIR, f)) as fp:
                orders.append({"filename": f, "data": json.load(fp)})
    return jsonify(orders)


if __name__ == '__main__':
    app.run(debug=True, port=5000)