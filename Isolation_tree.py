import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt

# 假設已載入停車數據
data = pd.read_csv("世正經貿大樓.csv")

# 計算停車時長
data['duration'] = (pd.to_datetime(data['order_end_time']) - pd.to_datetime(data['order_start_time'])).dt.total_seconds() / 3600

# 基於停車時長檢測異常 (簡單規則)
data['duration_anomaly'] = data['duration'].apply(lambda x: x > 24 or x < 0)

# 使用 Isolation Forest 進行異常檢測
iso = IsolationForest(contamination=0.05, random_state=42)
data['if_anomaly'] = iso.fit_predict(data[['duration', 'payment_amount']])

# 可視化
plt.scatter(data['duration'], data['payment_amount'], c=data['if_anomaly'], cmap='coolwarm')
plt.xlabel('Parking Duration (hours)')
plt.ylabel('Amount Charged')
plt.title('Anomaly Detection in Parking Data')
plt.show()
