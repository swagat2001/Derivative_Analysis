# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 19:17:01 2024

@author: Niraj
"""

import datetime
import os
import re
import time
import traceback
from urllib.request import Request, urlopen

import pandas as pd
import requests
from bs4 import BeautifulSoup


class stockScreener:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"
    }
    baseurl = "https://www.screener.in/"
    bseHeaders = {
        "Access-Control-Allow-Origin": "*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.bseindia.com",
        "Referer": "https://www.bseindia.com/",
    }
    bseBaseurl = "https://api.bseindia.com"
    endpoints = {
        "base": "company/{token}/{consolidated}/",
        "chart": "/api/company/{screenerID}/chart/?q=Price-DMA50-DMA200-Volume&days=10000{consolidated}",  # &consolidated=true
        "quarterlyReport": {
            "Sales": "/api/company/{screenerID}/schedules/?parent=Sales&section=quarters{consolidated}",  # &consolidated=
            "Expenses": "api/company/{screenerID}/schedules/?parent=Expenses&section=quarters{consolidated}",
            "OtherIncomne": "api/company/{screenerID}/schedules/?parent=Other+Income&section=quarters{consolidated}",
            "NetProfit": "/api/company/{screenerID}/schedules/?parent=Net+Profit&section=quarters{consolidated}",
        },
        "pnlReport": {
            "Sales": "/api/company/{screenerID}/schedules/?parent=Sales&section=profit-loss{consolidated}",
            "Expenses": "/api/company/{screenerID}/schedules/?parent=Expenses&section=profit-loss{consolidated}",
            "OtherIncome": "/api/company/{screenerID}/schedules/?parent=Other+Income&section=profit-loss{consolidated}",
            "NetProfit": "/api/company/{screenerID}/schedules/?parent=Net+Profit&section=profit-loss{consolidated}",
            "MaterialCost": "/api/company/{screenerID}/schedules/?parent=Material+Cost+%25&section=profit-loss{consolidated}",
        },
        "balanceSheet": {
            "Borrowing": "/api/company/{screenerID}/schedules/?parent=Borrowings&section=balance-sheet{consolidated}",
            "TotalAssets": "/api/company/{screenerID}/schedules/?parent=Fixed+Assets&section=balance-sheet{consolidated}",
            "OtherLiabilities": "/api/company/{screenerID}/schedules/?parent=Other+Liabilities&section=balance-sheet{consolidated}",
            "OtherAssets": "/api/company/{screenerID}/schedules/?parent=Other+Assets&section=balance-sheet{consolidated}",
        },
        "cashFlow": {
            "OperatingAct": "/api/company/{screenerID}/schedules/?parent=Cash+from+Operating+Activity&section=cash-flow{consolidated}",
            "FinancingAct": "/api/company/{screenerID}/schedules/?parent=Cash+from+Financing+Activity&section=cash-flow{consolidated}",
            "InvestingAct": "/api/company/{screenerID}/schedules/?parent=Cash+from+Investing+Activity&section=cash-flow{consolidated}",
        },
        "shareHolding": {
            "Promoters": "/api/3/{screenerID}/investors/promoters/{duration}/",  # duration #yearly
            "FII": "/api/3/{screenerID}/investors/foreign_institutions/{duration}/",
            "DII": "/api/3/{screenerID}/investors/domestic_institutions/{duration}/",
            "GOV": "/api/3/{screenerID}/investors/government/{duration}/",
            "Public": "/api/3/{screenerID}/investors/public/{duration}/",
        },
        "corporateAnnouncements": "/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno={pageno}&strCat=-1&strPrevDate={prevDate}&strScrip={token}&strSearch=P&strToDate={toDate}&strType=C&subcategory=-1",  # strType : EDDA
        "upcomingAnnoucements": "/BseIndiaAPI/api/Corpforthresults/w",
    }  # 20240420 #500325 #20240720
    __today = datetime.datetime.now().date()

    def __init__(self, token=None, consolidated=True):
        self.reqSession = requests.Session()
        if token:
            baseurl = self.baseurl + self.endpoints["base"]
            self.url = baseurl.format(token=token, consolidated="" if not consolidated else "consolidated")
            content = self.requestAPI(method="GET", url=self.url, content=True)
            self.soup = BeautifulSoup(content, "html.parser")
            self.screenerID = self.getID()
            self.token = token
            self.__consoltag = "&consolidated=" if consolidated else ""

    def getID(self):
        a = self.soup.find_all("style")
        data = re.findall(r'data-row-company-id="(\d+)"', str(a[0]))
        if data != []:
            return data[0]
        else:
            raise Exception("Unable to find screener ID")

    def requestAPI(self, method, url, content=False):
        resp = self.reqSession.request(method, url, headers=self.headers)
        if resp.status_code == 200:
            if content:
                return resp.content
            else:
                resp = resp.json()
                return resp

    def getEndpoint(self, section):
        sections = {
            "quarters": "quarterlyReport",
            "profit-loss": "pnlReport",
            "balance-sheet": "balanceSheet",
            "cash-flow": "cashFlow",
            "yearly-shp": "shareHolding",
            "quarterly-shp": "shareHolding",
        }
        return sections[section]

    def __pullData(self, id, withAddon=False, _class="data-table responsive-text-nowrap"):
        section = self.soup.find(id=id)
        section = section.find(class_=_class)
        data = {}
        headers = []
        for th in section.find_all("th"):
            dtstr = th.text.strip()
            if dtstr != "":
                if dtstr != "TTM":
                    dt = str(datetime.datetime.strptime(dtstr, "%b %Y").date())
                else:
                    dt = dtstr
                headers.append(dt)
                data[dt] = []

        for tr in section.find_all("tr")[1:]:  # Skip the header row
            cells = tr.find_all("td")
            for i, cell in enumerate(cells):
                if i == 0:
                    key = cell.text.strip().replace(" ", "").replace("+", "")
                else:
                    if "hover-link" in cell.get("class", []):
                        a_tag = cell.find("a")
                        data[headers[i - 1]].append({key: a_tag.get("href")})
                    else:
                        value = cell.text.strip().replace(",", "")
                        if value.find("%") != -1:
                            value = value.replace("%", "")
                            value = float(value) / 100
                        value = 0 if value == "" else value
                        data[headers[i - 1]].append({key: float(value)})
        if withAddon:
            tag = self.getEndpoint(id)
            duration = False if tag != "shareHolding" else "quarterly" if "quarterly" in id else "yearly"
            data = self.__addonData(data, self.getEndpoint(id), duration=duration)
        return data

    def __addonData(self, data, section, duration=False):
        tags = list(self.endpoints[section].keys())
        for tag in tags:
            try:
                if section == "shareHolding":
                    url = self.baseurl + self.endpoints[section][tag].format(
                        screenerID=self.screenerID, duration=duration
                    )
                    pass
                else:
                    url = self.baseurl + self.endpoints[section][tag].format(
                        screenerID=self.screenerID, consolidated=self.__consoltag
                    )
                resp = self.requestAPI("GET", url)
                if resp is None:
                    continue
                for key in resp:
                    _key = key.replace(" ", "")  # .replace("%", "")
                    values = resp[key]
                    for val in values:
                        try:
                            _val = str(datetime.datetime.strptime(val, "%b %Y").date())
                            if _val in data.keys():
                                _valkey = values[val].replace(" ", "").replace(",", "")
                                if "%" in key:
                                    _valkey = float(_valkey.replace("%", ""))
                                    _valkey = _valkey / 100
                                else:
                                    _valkey = float(_valkey)
                                data[_val].append({_key: _valkey})
                        except Exception:
                            pass

            except Exception as e:
                print(traceback.print_exc())
                print(e)
        return data

    def quarterlyReport(self, withAddon=False):
        return self.__pullData("quarters", withAddon=withAddon)

    def pnlReport(self, withAddon=False):
        return self.__pullData("profit-loss", withAddon=withAddon)

    def balanceSheet(self, withAddon=False):
        return self.__pullData("balance-sheet", withAddon=withAddon)

    def cashFLow(self, withAddon=False):
        return self.__pullData("cash-flow", withAddon=withAddon)

    def ratios(self):
        return self.__pullData("ratios")

    def shareHolding(self, quarterly=False, withAddon=False):
        tag = "yearly-shp" if not quarterly else "quarterly-shp"
        return self.__pullData(tag, _class="responsive-holder fill-card-width", withAddon=withAddon)

    def extract_year(self, text):
        pattern = r"Year (\d{4})"
        match = re.search(pattern, text)
        if match:
            return match.group(1)
        else:
            print(text)
            return None

    def annualReports(self):
        pattern = re.compile(r"plausible-event-name=Annual\+Report")
        data = self.soup.find_all("a", class_=pattern)
        resp = {}
        for report in data:
            url = report.get("href")
            name = report.get_text(strip=True)
            year = self.extract_year(name)
            dt = str(datetime.datetime.strptime(year, "%Y").date())
            resp[dt] = url
        return resp

    def closePrice(self):
        url = self.baseurl + self.endpoints["chart"].format(screenerID=self.screenerID, consolidated=self.__consoltag)
        return self.requestAPI("GET", url)

    def __corporateAnnouncements(self, prevDate, toDate):
        prevDate = prevDate.strftime("%Y%m%d")
        toDate = toDate.strftime("%Y%m%d")
        all_data = []
        url = self.bseBaseurl + self.endpoints["corporateAnnouncements"].format(
            pageno=1, prevDate=prevDate, toDate=toDate, token=self.token
        )
        data = self.requestBSE(url, "GET")
        all_data = all_data + data["Table"]
        rows = int(data["Table1"][0]["ROWCNT"])
        if rows >= 50:
            sets = rows // 50
            for i in range(sets):
                pageno = i + 2
                url = self.bseBaseurl + self.endpoints["corporateAnnouncements"].format(
                    pageno=pageno, prevDate=prevDate, toDate=toDate, token=self.token
                )
                data = self.requestBSE(url, "GET")
                all_data = all_data + data["Table"]
        return all_data

    def corporateAnnouncements(self, prevDate, toDate):
        if (toDate - prevDate).days >= 365:
            all_data = []
            while prevDate < toDate:
                nextDate = prevDate + datetime.timedelta(days=365)
                if nextDate >= toDate:
                    nextDate = toDate
                data = self.__corporateAnnouncements(prevDate, nextDate)
                all_data = all_data + data
                prevDate = nextDate + datetime.timedelta(days=1)
            return all_data

        else:
            data = self.__corporateAnnouncements(prevDate, toDate)
            return data

    def latestAnnouncements(self, date=__today):
        prevDate = date.strftime("%Y%m%d")
        toDate = date.strftime("%Y%m%d")
        all_data = []
        url = self.bseBaseurl + self.endpoints["corporateAnnouncements"].format(
            pageno=1, prevDate=prevDate, toDate=toDate, token=""
        )
        data = self.requestBSE(url, "GET")
        all_data = all_data + data["Table"]
        rows = int(data["Table1"][0]["ROWCNT"])
        if rows >= 50:
            sets = rows // 50
            for i in range(sets):
                pageno = i + 2
                url = self.bseBaseurl + self.endpoints["corporateAnnouncements"].format(
                    pageno=pageno, prevDate=prevDate, toDate=toDate, token=""
                )
                data = self.requestBSE(url, "GET")
                all_data = all_data + data["Table"]
        return all_data

    def upcomingResults(self):
        url = self.bseBaseurl + self.endpoints["upcomingAnnoucements"]
        return self.requestBSE(url, "GET")

    def requestBSE(self, url, method, content=False):
        resp = self.reqSession.request(method, url, headers=self.bseHeaders)
        if resp.status_code == 200:
            if content:
                return resp.content
            else:
                resp = resp.json()
                return resp

    def concallTranscript(self):
        content = self.soup.find_all(class_="concall-link")
        data = {}
        print(len(content))
        for item in content:
            date_text = item.find("div", class_="ink-600 font-size-15 font-weight-500 nowrap").text.strip()
            transcript_link = item.find("a", class_="concall-link", title="Raw Transcript")
            # if transcript_link:
            dta = str(datetime.datetime.strptime(date_text, "%b %Y").date())
            print(dta)
            data[dta] = transcript_link["href"]
        return data

    def getCompanyInfo(self):
        """Scrape company info header (Market Cap, High/Low, etc.)"""
        ratios_div = self.soup.find('ul', id='top-ratios')
        if not ratios_div:
            return {}

        data = {}
        for li in ratios_div.find_all('li'):
            name = li.find('span', class_='name').text.strip()
            # Handle multiple numbers (like High / Low)
            numbers = li.find_all('span', class_='number')
            if len(numbers) > 1:
                # Join with ' / ' to match the key format or just clean strings
                val = " / ".join([n.text.strip().replace(',', '') for n in numbers])
            else:
                val = li.find('span', class_='number').text.strip().replace(',', '')
            data[name] = val
        return data


class ScreenerScrape(stockScreener):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKENS_DIR = os.path.join(BASE_DIR, "tokens")

    if not os.path.exists(TOKENS_DIR):
        os.makedirs(TOKENS_DIR)

    def __init__(self):
        try:
            self.tokendf = pd.read_csv(
                os.path.join(self.TOKENS_DIR, "tokens_{dt}.csv".format(dt=datetime.datetime.now().strftime("%Y%m%d")))
            )
        except Exception:
            if os.path.exists(self.TOKENS_DIR):
                files = os.listdir(self.TOKENS_DIR)
                for file in files:
                    os.remove(os.path.join(self.TOKENS_DIR, file))
            self.getTokendf()

    def getTokendf(self):
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        r = requests.get(url, timeout=60)
        df = pd.DataFrame(r.json())
        self.tokendf = df[df["exch_seg"] == "BSE"][["token", "name", "symbol"]]
        self.tokendf.to_csv(os.path.join(self.TOKENS_DIR, "tokens_{dt}.csv".format(dt=datetime.datetime.now().strftime("%Y%m%d"))))

    def getBSEToken(self, symbol):
        data = self.tokendf[self.tokendf["symbol"] == symbol]
        if not data.empty:
            return str(data.iloc[0]["token"])
        else:
            return False

    def loadScraper(self, token, consolidated=True):
        super().__init__(token=token, consolidated=consolidated)

    def latestAnnouncements(self, date=None):
        sc = stockScreener()
        if date is not None:
            return sc.latestAnnouncements(date=date)
        else:
            return sc.latestAnnouncements()

    def upcomingResults(self):
        sc = stockScreener()
        return sc.upcomingResults()

    def companyInfo(self):
        return self.getCompanyInfo()


# quarterly DF
# if df :
#     data = pd.read_html(str(section))[0]
#     data[data.columns[0]] = data[data.columns[0]].str.replace(" ", "").str.replace("%", "")
#     data = data.set_index('Unnamed: 0')


# def _quarterlyReport(self, withAddon = False):
#     def getAddon(data):
#         section = "quarterlyReport"
#         tags = list(self.endpoints[section].keys())
#         for tag in tags :
#             try :
#                 url = self.baseurl + self.endpoints[section][tag].format(screenerID = self.screenerID, consolidated=self.__consoltag)
#                 resp = self.requestAPI("GET", url)
#                 for key in resp :
#                     _key = key.replace(" ", "")#.replace("%", "")
#                     values = resp[key]
#                     for val in values :
#                         _val = str(datetime.datetime.strptime(val, "%b %Y").date())
#                         if _val in data.keys():
#                             _valkey = values[val].replace(" ", "").replace(",", "")
#                             if "%" in key:
#                                 _valkey = float(_valkey.replace("%", ""))
#                                 _valkey = _valkey/100
#                             else:
#                                 _valkey = float(_valkey)
#                             data[_val].append({_key : _valkey})
#             except Exception as e:
#                 print(traceback.print_exc())
#                 print(e)
#         return data

#     quarterly_report_section = self.soup.find(id='quarters')
#     section = quarterly_report_section.find(class_="data-table responsive-text-nowrap")
#     data = {}
#     headers = []
#     for th in section.find_all('th'):
#         dtstr = th.text.strip()
#         if dtstr != "" :
#             dt = str(datetime.datetime.strptime(dtstr, "%b %Y").date())
#             headers.append(dt)
#             data[dt] = []

#     for tr in section.find_all('tr')[1:]:  # Skip the header row
#         cells = tr.find_all('td')
#         for i, cell in enumerate(cells):
#             if i == 0 :
#                 key = cell.text.strip().replace(" ", "").replace("+", "")
#             else:
#                 if "hover-link" in cell.get("class",[]):
#                     a_tag = cell.find('a')
#                     data[headers[i-1]].append({key: a_tag.get('href')})
#                 else:
#                     value = cell.text.strip().replace(",","")
#                     if value.find("%") != -1 :
#                         value = value.replace("%","")
#                         value = float(value)/100

#                     data[headers[i-1]].append({key: float(value)})

#     if withAddon:
#         data = getAddon(data)
#     return data

# def _pnlstatement(self, withAddon = False):
#     def getAddon(data):
#         section = "pnlReport"
#         tags = list(self.endpoints[section].keys())
#         for tag in tags :
#             try :
#                 url = self.baseurl + self.endpoints[section][tag].format(screenerID = self.screenerID, consolidated=self.__consoltag)
#                 resp = self.requestAPI("GET", url)
#                 for key in resp :
#                     _key = key.replace(" ", "")#.replace("%", "")
#                     values = resp[key]
#                     for val in values :
#                         try:
#                             _val = str(datetime.datetime.strptime(val, "%b %Y").date())
#                             if _val in data.keys():
#                                 _valkey = values[val].replace(" ", "").replace(",", "")
#                                 if "%" in key:
#                                     _valkey = float(_valkey.replace("%", ""))
#                                     _valkey = _valkey/100
#                                 else:
#                                     _valkey = float(_valkey)
#                                 data[_val].append({_key : _valkey})
#                         except:
#                             pass

#             except Exception as e:
#                 print(traceback.print_exc())
#                 print(e)
#         return data

#     quarterly_report_section = self.soup.find(id='profit-loss')
#     section = quarterly_report_section.find(class_="data-table responsive-text-nowrap")
#     data = {}
#     headers = []
#     for th in section.find_all('th'):
#         dtstr = th.text.strip()
#         if dtstr != "" :
#             if dtstr != "TTM" :
#                 dt = str(datetime.datetime.strptime(dtstr, "%b %Y").date())
#             else:
#                 dt = dtstr
#             headers.append(dt)
#             data[dt] = []

#     for tr in section.find_all('tr')[1:]:  # Skip the header row
#         cells = tr.find_all('td')
#         for i, cell in enumerate(cells):
#             if i == 0 :
#                 key = cell.text.strip().replace(" ", "").replace("+", "")
#             else:
#                 if "hover-link" in cell.get("class",[]):
#                     a_tag = cell.find('a')
#                     data[headers[i-1]].append({key: a_tag.get('href')})
#                 else:
#                     value = cell.text.strip().replace(",","")
#                     if value.find("%") != -1 :
#                         value = value.replace("%","")
#                         value = float(value)/100
#                     value = 0 if value == "" else value
#                     data[headers[i-1]].append({key: float(value)})
#     if withAddon:
#         data = getAddon(data)
#     return data
