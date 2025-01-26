import requests
import pandas as pd
import json
import time
import random
from sqlalchemy.types import VARCHAR, INTEGER, TEXT, DATE, FLOAT
from urllib.parse import quote_plus
import logging
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class JobScraper:
    def __init__(self, host, port, user, password, db):
        self.city_codes = {
            "台北市": "6001001000"
        }
        self.job_codes = {
            "儲備幹部": "2001001002"
            # "經營管理主管": "2001001001"
        }
        self.session = self._create_session()
        self._init_headers()
        self.base_wait_time = 3

        # MySQL 連線設定
        db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
        self.engine = create_engine(db_url, pool_pre_ping=True)

    def _init_headers(self):
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Host': 'www.104.com.tw',
            'Origin': 'https://www.104.com.tw',
            'Referer': 'https://www.104.com.tw/jobs/search/',
            'User-Agent': self._get_random_ua()
        }

    def _get_random_ua(self):
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0'
        ]
        return random.choice(user_agents)

    def _create_session(self):
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _exponential_backoff(self, attempt):
        """計算指數退避時間，防止請求過於頻繁"""
        return min(self.base_wait_time * (2 ** attempt) + random.uniform(0, 1), 60)

    def get_request(self, url, params=None, headers=None, attempt=0):
        if not headers:
            headers = self.headers

        try:
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logging.error(f"404 Not Found for URL: {url}. Skipping this job.")
                return None
            logging.error(f"Request failed for URL: {url} with params: {params}. Error: {str(e)}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for URL: {url} with params: {params}. Error: {str(e)}")
            if attempt < 5:
                wait_time = self._exponential_backoff(attempt)
                logging.info(f"Retrying in {wait_time:.2f} seconds... (Attempt {attempt + 1}/5)")
                time.sleep(wait_time)
                return self.get_request(url, params, headers, attempt + 1)
        return None

    def fetch_jobs(self, city_code, job_code):
        url = 'https://www.104.com.tw/jobs/search/list'
        all_jobs = []
        job_name = next((key for key, value in self.job_codes.items() if value == job_code), None)

        max_pages = 1
        for page in range(1, max_pages + 1):
            params = {
                'ro': '0',
                'kwop': '7',
                'keyword': '',
                'order': '15',
                'asc': '0',
                'page': str(page),
                'mode': 'l',
                'jobsource': '2018indexpoc',
                'area': city_code,
                'jobcat': job_code
            }

            logging.info(f"Fetching page {page} for job_code {job_code} ({job_name}) in city_code {city_code}...")
            response = self.get_request(url, params=params)

            if not response or 'data' not in response or 'list' not in response['data']:
                logging.warning(f"Unexpected response format: {response}")
                break

            jobs = response['data']['list']
            for job in jobs:
                job['JobCat'] = job_name
                link_data = job.get('link', {})
                apply_analyze = link_data.get('applyAnalyze', '')

                if apply_analyze:
                    job_code = apply_analyze.split('/')[6].split('?')[0]
                    job['code'] = job_code
                else:
                    logging.warning(f"Missing 'applyAnalyze' in job link: {link_data}")
                    continue

                header = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Host': 'www.104.com.tw',
                    'Referer': 'https://www.104.com.tw/',
                    'User-Agent': self._get_random_ua()
                }
                job_detail_url = f"https://www.104.com.tw/job/ajax/content/{job['code']}"
                rep = self.get_request(job_detail_url, headers=header)
                
                if rep and 'data' in rep:
                    job['condition'] = rep['data'].get('condition', {})
                    job['jobCategory'] = rep['data']['jobDetail'].get('jobCategory', {})
    
                    # 抓取公司的 URL
                    cust_url = rep['data']['header'].get('custUrl', None)
                    if cust_url:
                        company_code = cust_url.split('/')[-1]  # 取出公司的 code
                        company_url = f"https://www.104.com.tw/company/ajax/content/{company_code}"
        
                        company_response = self.get_request(company_url)
                        if company_response and 'data' in company_response:
                            job['company_employees'] = company_response['data'].get('empNo', 'N/A')
                            job['company_capital'] = company_response['data'].get('capital', 'N/A')
                        else:
                            logging.warning(f"Failed to fetch company details for company_code: {company_code}")
                    else:
                        logging.warning(f"Missing 'custUrl' in response header: {rep['data']['header']}")
                else:
                    logging.warning(f"Failed to fetch job details for code: {job['code']}")


            all_jobs.extend(jobs)
            time.sleep(random.uniform(2, 5))

        logging.info(f"Total jobs fetched for job_code {job_code} ({job_name}): {len(all_jobs)}")
        return all_jobs

    def save_to_csv(self, df, filename):
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logging.info(f"Successfully saved data to {filename}")
        except Exception as e:
            logging.error(f"Error saving to csv: {str(e)}")

    def map_dataframe_to_db(self, df):
        # Define the mapping from DataFrame columns to database fields with types
        dtype_mapping = {
            'jobType': VARCHAR(255),
            'jobNo': VARCHAR(255),
            'jobName': VARCHAR(255),
            'jobNameSnippet': TEXT,
            'jobRole': VARCHAR(255),
            'jobRo': VARCHAR(255),
            'jobAddrNo': VARCHAR(255),
            'jobAddrNoDesc': VARCHAR(255),
            'jobAddress': TEXT,
            'description': TEXT,
            'descWithoutHighlight': TEXT,
            'optionEdu': VARCHAR(255),
            'period': VARCHAR(255),
            'periodDesc': VARCHAR(255),
            'applyCnt': INTEGER,
            'applyType': VARCHAR(255),
            'applyDesc': VARCHAR(255),
            'custNo': VARCHAR(255),
            'custName': VARCHAR(255),
            'coIndustry': VARCHAR(255),
            'coIndustryDesc': VARCHAR(255),
            'salaryLow': INTEGER,
            'salaryHigh': INTEGER,
            'salaryDesc': VARCHAR(255),
            's10': VARCHAR(255),
            'appearDate': DATE,
            'appearDateDesc': VARCHAR(255),
            'optionZone': VARCHAR(255),
            'isApply':  INTEGER,
            'applyDate': DATE,
            'isSave':  INTEGER,
            'descSnippet': TEXT,
            'tags': TEXT,
            'landmark': VARCHAR(255),
            'link': TEXT,
            'jobsource': VARCHAR(255),
            'jobNameRaw': TEXT,
            'custNameRaw': TEXT,
            'lon': FLOAT,
            'lat': FLOAT,
            'remoteWorkType': VARCHAR(255),
            'major': VARCHAR(255),
            'salaryType': VARCHAR(255),
            'dist': VARCHAR(255),
            'mrt': VARCHAR(255),
            'mrtDesc': VARCHAR(255),
            'JobCat': VARCHAR(255),
            'code': VARCHAR(255),
            'condition': TEXT,
            'jobCategory': TEXT,
            'company_employees': VARCHAR(255),
            'company_capital': VARCHAR(255)
        }

        # Create a new DataFrame with the mapped columns
        mapped_df = df.rename(columns={col: col for col in dtype_mapping.keys()})

        return mapped_df

    def save_to_mysql(self, df):
        if df.empty:
            logging.info("No data to save to MySQL.")
            return
        try:
        # Map the DataFrame to the database structure
            mapped_df = self.map_dataframe_to_db(df)

        # 找出並轉換包含 dict 或 list 的欄位
            dict_columns = mapped_df.applymap(lambda x: isinstance(x, dict)).any()
            list_columns = mapped_df.applymap(lambda x: isinstance(x, list)).any()

            for col in dict_columns[dict_columns].index.tolist():
                mapped_df[col] = mapped_df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
        
            for col in list_columns[list_columns].index.tolist():
                mapped_df[col] = mapped_df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

            MYSQL_HOST = '127.0.0.1'
            MYSQL_PORT = '3306'
            MYSQL_USER = 'root'
            MYSQL_PASSWORD = quote_plus(MYSQL_PASSWORD)
            MYSQL_DB = MYSQL_DB
            db_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

            self.engine = create_engine(db_url, pool_pre_ping=True)

            with self.engine.connect() as conn:
                logging.info("Database connection successful.")

            # 插入資料時使用 method='multi'
            mapped_df.to_sql('jobs', con=self.engine, if_exists='append', index=False, method='multi')
            logging.info("Successfully saved data to MySQL.")
    
        except Exception as e:
            logging.error(f"Error saving to MySQL: {str(e)}", exc_info=True)


    def run(self):
        for city_name, city_code in self.city_codes.items():
            all_jobs = []

            for job_name, job_code in self.job_codes.items():
                logging.info(f"Fetching data for {city_name} - {job_name}")
                try:
                    jobs = self.fetch_jobs(city_code, job_code)
                    if jobs:
                        all_jobs.extend(jobs)
                        time.sleep(random.uniform(20, 30))
                except Exception as e:
                    logging.error(f"Error processing {city_name} - {job_name}: {str(e)}")
                    continue

            if all_jobs:
                filename = f'./job_104_data_{city_name}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                df = pd.DataFrame(all_jobs)
                #logging.info(df.dtypes)
                # print(df.dtypes)
                #self.save_to_csv(df, filename)
                self.save_to_mysql(df)  # 新增插入到 MySQL 的步驟


if __name__ == "__main__":
    MYSQL_HOST = 'localhost'
    MYSQL_PORT = '3306'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = MYSQL_PASSWORD
    MYSQL_DB = MYSQL_DB
    scraper = JobScraper(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB)
    scraper.run()
