# from Analysis_Tools.app import create_app
# from flask import Flask, render_template
# from flask_socketio import SocketIO, emit
# import csv, os, time, threading


# app = Flask(__name__)
# app.config['SECRET_KEY'] = 'secret!'
# socketio = SocketIO(app)  # Integrates Socket.IO into Flask

# CSV_PATH = r"C:\Users\Admin\Desktop\Derivative_Analysis\spot_data\Data\spotData.csv"

# def read_last_row():
#     with open(CSV_PATH, 'r') as f:
#         reader = csv.DictReader(f)
#         rows = list(reader)
#         return rows[-1] if rows else {}

# def watch_csv():
#     """Continuously watch CSV and broadcast new ticks"""
#     last_mtime = 0
#     last_data = None
#     while True:
#         try:
#             if not os.path.exists(CSV_PATH):
#                 time.sleep(1)
#                 continue
#             mtime = os.path.getmtime(CSV_PATH)
#             if mtime != last_mtime:
#                 last_mtime = mtime
#                 new_data = read_last_row()
#                 if new_data != last_data:
#                     last_data = new_data
#                     socketio.emit("new_tick", new_data)
#             time.sleep(0.3)
#         except Exception as e:
#             print("Error watching CSV:", e)
#             time.sleep(1)

# @app.route("/")
# def dashboard():
#     print("Rendering dashboard")
#     indices = {"NIFTY": 25903.05, "BANKNIFTY": 58201.1, "GOLD": 120146.0, "SILVER": 145573.0}
#     return render_template("views/dashboard.html")


# # app = create_app()

# # if __name__ == "__main__":
# #     app.run("0.0.0.0", port=5000, debug=True)

# if __name__ == '__main__':
#     socketio.run(app, debug=True)  # Use socketio.run instead of app.run


from Analysis_Tools.app import create_app

app = create_app()

if __name__ == "__main__":
    app.run("0.0.0.0", port=5000, debug=True)
