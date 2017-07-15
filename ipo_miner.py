from dateutil.relativedelta import relativedelta
from pandas_datareader import data as web
from datetime import datetime, timedelta
from argparse import ArgumentParser
from bs4 import BeautifulSoup
import requests
import dateutil
import logging
import json
import time
import re

logging.basicConfig(level=logging.INFO, filename='ipo_miner.log')


class Miner(object):
	NASDAQ_IPO_PRICING_URL = 'http://www.nasdaq.com/markets/ipos/activity.aspx?tab=pricings&month={year}-{month:02}'
	PRICING_TABLE_CLASS = 'genTable'
	COOKIE_TEMPLATE = '__atuvc=1%7C27; __qca=P0-967455184-1499228559933; __gads=ID=feac94a6304e5b52:T=1499228562:S=ALNI_MabEbn4ABIaoobpwo81uQawy0nwjQ; userSymbolList=AKCA+; userCookiePref=true; _dy_df_geo=Israel..Netanya; m0r9h.salt=MOREPHEUS21$; c_enabled$=true; i10cNonce=-TU9SRVBIRVVTMjEkMTUwMDAxNTk4NDE2OQ==; i10c3C=0; m0r9h.bsalt=MOREPHEUS21$,1500017194746; s_sess=%20s_cc%3Dtrue%3B%20s_sq%3D%3B; _dyid=5246693760659384719; _dycst=dk.w.c.ws.frv5.tos.; _dy_geo=IL.AS.IL_02.IL_02_Netanya; _dy_df_geo=Israel..Netanya; _dy_toffset=0; s_pers=%20bc%3D2%7C1500105765606%3B%20s_nr%3D1500019979514-Repeat%7C1507795979514%3B; _dyus_8767356=142%7C0%7C0%7C0%7C0%7C0.0.1499228560811.1500019818940.791258.0%7C194%7C28%7C6%7C117%7C31%7C0%7C0%7C0%7C0%7C0%7C0%7C31%7C4%7C0%7C0%7C0%7C0%7C35%7C0%7C0%7C0%7C0%7C0; ADRUM_BT=R%3a61%7cclientRequestGUID%3a01eca8c4-0657-46c3-9e2c-8311a29e202a%7cbtId%3a81368%7cbtERT%3a96; NSC_W.TJUFEFGFOEFS.OBTEBR.80=ffffffffc3a0f73345525d5f4f58455e445a4a423660; clientPrefs=||||lightg; _dy_csc_ses=t; _dy_ses_load_seq=89400%3A1500020829898; _dy_c_exps=; _dy_soct=109626.151069.1500020829; i10c.referrer={referer}'
	HEADERS = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3153.0 Safari/537.36',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
		'Referer': '',
		'Accept-Language': 'en-US,en;q=0.8,he;q=0.6',
		'Cookie': COOKIE_TEMPLATE.format(referer='')
	}

	def __init__(self):
		self.logger = logging.getLogger('miner')
		self.logger.setLevel(logging.INFO)
		fh = logging.FileHandler('ipo_miner.log')
		ch = logging.StreamHandler()
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		fh.setFormatter(formatter)
		ch.setFormatter(formatter)
		self.logger.addHandler(fh)
		self.logger.addHandler(ch)
		self.logger.info('miner initialized')

	def mine_to(self, file_name, from_date, to_date):
		self.logger.info('mining to - {}'.format(file_name))

		try:
			entries = json.load(open(file_name))
		except:
			entries = []

		month, year = to_date.month, to_date.year
		months_to_mine = (year - from_date.year) * 12 + (month - from_date.month) + 1
		if months_to_mine < 0 or from_date > to_date:
			return self.logger.warn('from date seems bigger than to date - cancelling operation')

		for i in xrange(months_to_mine):
			url = self.NASDAQ_IPO_PRICING_URL.format(year=year, month=month)
			self.logger.info('mining month - {} year - {} url - {}'.format(month, year, url))
			monthly_entries = self._mine_from_url(url)
			entries.extend(monthly_entries)
			
			self.logger.info('successfully mined {} entries for month {}'.format(len(monthly_entries), month))
			month = 12 if month <= 1 else month - 1
			year = year - 1 if month == 12 else year
			self.HEADERS['Referer'] = url
			self.HEADERS['Cookie'] = self.COOKIE_TEMPLATE.format(referer=url)
			time.sleep(1)
			with open(file_name, 'w') as f:
				f.write(json.dumps(entries))

		self.logger.info('done - mined a total of {} entries to {}'.format(len(entries), file_name))

	def _mine_from_url(self, url):
		html = BeautifulSoup(requests.get(url, headers=self.HEADERS).text, "html5lib")
		self.logger.info('searching for table with class - {}'.format(self.PRICING_TABLE_CLASS))
		table = html.find('div', {'class': 'genTable'})
		if not table or not table.text or not table.tbody or not table.tbody.text or not table.tbody.find_all('tr'):
			self.logger.warn('did not find anything for url - {}; skipping'.format(url))
			return []

		entries = []

		rows = table.tbody.find_all('tr')
		for i, row in enumerate(rows):
			columns = row.find_all('td')
			if not columns:
				self.logger.warn('skipping entry #{} - no columns found'.format(i))
				continue

			company = columns[0].a.text
			company_url = columns[0].a.attrs['href']
			symbol = columns[1].a.text
			market = columns[2].text
			price = columns[3].text
			shares = columns[4].text
			amount = columns[5].text
			date = columns[6].text
			price_num = float(re.sub('[^\d\.]+', '', price))
			self.logger.info('Mining entry #{} for company - {}'.format(i, company))
			try:
				ipo_data = self._mine_company_url(company_url)
			except Exception as e:
				self.logger.exception('failed fetching company data for - #{} - {}'.format(i, company))
				ipo_data = {}

			try:
				end_date = dateutil.parser.parse(date) + timedelta(days=1)
				trade_data = web.get_data_google(symbol, date, end_date.strftime('%m/%d/%Y'))
				first_day_open = trade_data.ix[0].Open
				first_day_close = trade_data.ix[0].Close
				first_day_change = float((first_day_close - first_day_open) / first_day_open * 100) if trade_data is not None else None
				first_day_ipo_change = float((first_day_close - price_num) / price_num * 100) if trade_data is not None else None
				first_day_positive = first_day_change > 0 if first_day_change is not None else None
				trade_data = json.loads(trade_data.to_json())
			except Exception as e:
				self.logger.exception('failed fetching finance data for - #{} - {}'.format(i, company))
				trade_data = {}
				first_day_change = None
				first_day_positive = None
				first_day_open = None
				first_day_close = None
				first_day_ipo_change = None

			entries.append({
				'company': company,
				'company_url': company_url,
				'symbol': symbol,
				'market': market,
				'price': price,
				'shares': shares,
				'amount': amount,
				'date': date,
				'ipo_data': ipo_data,
				'trade_data': trade_data,
				'first_day_market_change': first_day_change,
				'first_day_market_positive': first_day_positive,
				'first_day_open': first_day_open,
				'first_day_close': first_day_close,
				'price_num': price_num,
				'first_day_ipo_change': first_day_ipo_change
			})

		return entries

	def _mine_company_url(self, url):
		self.logger.info('mining company url - {}'.format(url))
		html = BeautifulSoup(requests.get(url, headers=self.HEADERS).text, 'html5lib')
		if not html or not html.text:
			self.logger.warn('failed to parse html for company url - {}'.format(url))
			return {}

		# Get summary table
		summary_table = html.find('div', {'id': 'infoTable'})
		if not summary_table or not summary_table.text or not summary_table.find_all('tr'):
			self.logger.warn('failed parsing table for company url - {}'.format(url))

		entry = {}
		rows = summary_table.find_all('tr')
		for row in rows:
			columns = summary_table.find_all('td')
			if not columns or not len(columns) % 2 == 0:
				self.logger.warn('could not find columns in summary table for company url - {}'.format(url))
				continue

			entry[columns[0].text.lower()] = columns[1].text.lower()

		# Get short company description
		company_description = html.find('div', {'class': 'ipo-comp-description'})
		if not company_description or not company_description.text or not company_description.pre or not company_description.pre.text:
			self.logger.warn('did not find company description for company url - {}'.format(url))
			company_description = ''
		else:
			company_description = company_description.pre.text

		entry['description'] = company_description

		# Get use of proceeds
		use_of_proceeds = html.find('div', {'id': 'infoTable_2'})
		if not use_of_proceeds or not use_of_proceeds.text or not use_of_proceeds.pre or not use_of_proceeds.pre.text:
			self.logger.warn('did not find use of proceeds for company url - {}'.format(url))
			use_of_proceeds = ''
		else:
			use_of_proceeds = use_of_proceeds.pre.text

		entry['use of proceeds'] = use_of_proceeds

		# Get competitors text
		competitors_text = html.find('div', {'id': 'infoTable_3'})
		if not competitors_text or not competitors_text.text or not competitors_text.pre or not competitors_text.pre.text:
			self.logger.warn('did not find competitor text for company url - {}'.format(url))
			competitors_text = ''
		else:
			competitors_text = competitors_text.pre.text

		entry['competitors_text'] = competitors_text

		# Get news headlines & count
		news_headlines = self._get_news_headlines(html)
		entry['news_headlines'] = news_headlines
		entry['news_headlines_count'] = len(news_headlines)

		# Get experts (underwriters) table
		experts = self._get_experts(html)
		entry['experts'] = experts

		# Get financials and filing
		financials_table = self._get_financials_table(html)
		entry['financials'] = financials_table

		return entry

	def _get_news_headlines(self, html):
		self.logger.info('fetching news headlines')
		
		news_div = html.find('div', {'id': 'CompanyNewsCommentary'})
		if not news_div or not news_div.text or not news_div.ul or not news_div.ul.text or not news_div.ul.find_all('li'):
			self.logger.warn('did not find news')
			return []

		news_entries = []

		news_items = news_div.ul.find_all('li')
		for news_item in news_items:
			news_url = news_item.a.attrs['href'] if news_item.a and news_item.a.text else ''
			news_headline = news_item.a.text if news_item.a and news_item.a.text else ''
			news_source = news_item.small.text if news_item.small and news_item.small.text else ''
			news_entry = {
				'news_url': news_url,
				'news_headline': news_headline,
				'news_source': news_source
			}
			news_entries.append(news_entry)
		
		return news_entries

	def _get_experts(self, html):
		self.logger.info('getting experts table')

		div = html.find('div', {'id': 'tabpane3'})
		table = div.find('div', {'class': 'genTable'})
		if not table or not table.text or not table.table or not table.table.text or not table.table.tbody or not table.table.tbody.text or not table.table.tbody.find_all('tr'):
			self.logger.warn('did not find experts table entries')
			return []

		experts = []
		rows = table.table.tbody.find_all('tr')
		for row in rows:
			columns = row.find_all('td')
			if not columns or not len(columns) % 2 == 0:
				self.logger.warn('did not find column or column length mismatch in experts table - {}'.format(url))
				continue

			expert_name = columns[1].a.text if columns[1].a and columns[1].a.text else ''
			expert_url = columns[1].a.attrs['href'] if columns[1].a and columns[1].a.text else ''
			experts.append({
				'type': columns[0].text,
				'expert_name': expert_name,
				'expert_url': expert_url
			})

		return experts

	def _get_financials_table(self, html):
		self.logger.info('getting financials table')

		div = html.find('div', {'id': 'tabpane2'})
		table = div.find('div', {'class': 'genTable'})
		if not div or not div.text or not table or not table.text:
			self.logger.warn('failed getting financials table')

		financials = {}
		tables = table.find_all('table')
		if not tables:
			self.logger.warn('failed getting financials and filing tables')

		income_table = tables[0]
		revenue = income_table.find('td', text=re.compile('revenue', re.I)).find_next('td') if income_table.find('td', text=re.compile('revenue', re.I)) else None
		net_income = income_table.find('td', text=re.compile('net income', re.I)).find_next('td') if income_table.find('td', text=re.compile('net income', re.I)) else None
		total_assets = income_table.find('td', text=re.compile('total assets', re.I)).find_next('td') if income_table.find('td', text=re.compile('total assets', re.I)) else None
		financials.update({
			'revenue': revenue.text if revenue else '',
			'net_income': net_income.text if net_income else '',
			'total_assets': total_assets.text if total_assets else ''
		})

		if len(tables) > 1:
			liabilities_table = tables[1]
			total_liabilities = liabilities_table.find('td', text=re.compile('total liabilities', re.I)).find_next('td') if liabilities_table.find('td', text=re.compile('total liabilities', re.I)) else None
			stockholders_equity = liabilities_table.find('td', text=re.compile('stockholders.*equity', re.I)).find_next('td') if liabilities_table.find('td', text=re.compile('stockholders.*equity', re.I)) else None
			financials.update({
				'total_liabilities': total_liabilities.text if total_liabilities else '',
				'stockholders_equity': stockholders_equity.text if stockholders_equity else ''
			})

		if len(tables) > 2:
			filings_table = tables[2]
			filings = []
			rows = filings_table.tbody.find_all('tr')
			for row in rows:
				columns = row.find_all('td')
				if not columns or not len(columns) % 4 == 0:
					self.logger.warn('invalid column in filings table; skipping')
					continue

				filings.append({
					'form_type': columns[1].text,
					'date_received': columns[2].text,
					'url': 'http://www.nasdaq.com{}'.format(columns[3].a.attrs['href']) if columns[3].a else ''
				})

			financials['filings'] = filings

		return financials


if '__main__' == __name__:
	parser = ArgumentParser()
	date_type = lambda d: datetime.strptime(d, '%m/%d/%Y')
	parser.add_argument('-o', dest='filename', help='output file', default='output.json')
	today = datetime.now()
	six_months_ago = today - relativedelta(months=6)
	parser.add_argument('-f', dest='from_date', help='from date (mm/dd/yyyy)', type=date_type, default=six_months_ago)
	parser.add_argument('-t', dest='to_date', help='to date (mm/dd/yyyy)', type=date_type, default=today)
	args = parser.parse_args()
	miner = Miner()
	miner.mine_to(args.filename, args.from_date, args.to_date)