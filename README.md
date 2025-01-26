# 104 人力銀行職缺爬蟲工具

此腳本用於爬取 104 人力銀行網站上的職缺資訊，包含職缺細節及公司資料，並將結果儲存為 CSV 檔案，方便進一步分析與使用。

## 功能特點
- **支援多城市與職缺分類**：可自訂爬取特定城市及職缺類別。
- **重試機制**：內建 HTTP 重試與指數回退機制，增強爬取穩定性。
- **詳細資料抓取**：包含職缺條件、職位類別、公司規模與資本額等資訊。
- **靈活設定**：支援動態 User-Agent 隨機切換，避免反爬機制。

## 需求
- Python 3.11.11
- 套件：`requests`, `pandas`, `logging`

安裝所需套件：
```bash
pip install requests pandas

# 104 職缺資料爬蟲與 MySQL 資料庫儲存工具

此工具用於爬取 104 人力銀行網站上的職缺資訊，並將結果儲存至 MySQL 資料庫，方便進行後續的數據分析。

## 功能特點
- **多城市與職缺類別支援**：可按城市與職缺類別進行爬取。
- **可靠的 HTTP 重試機制**：內建指數回退與重試策略，減少爬取中斷的風險。
- **數據庫操作**：將爬取的職缺數據轉存到 MySQL 資料庫中，結構化儲存便於查詢。
- **動態 User-Agent 切換**：模擬不同瀏覽器請求，繞過反爬機制。

## 系統需求
- Python 3.11.11
- MySQL 5.7 或以上版本
- 套件依賴：
  - `requests`
  - `pandas`
  - `pymysql`
  - `sqlalchemy`

安裝所需套件：
```bash
pip install requests pandas pymysql sqlalchemy

