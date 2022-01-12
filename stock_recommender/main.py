from stock_recommender import StockRecommender
from database_setup_class import Database

# Set up database instances 
nifty50_db = Database('NIFTY50')
dow_jones_db = Database('DJIA')

# Update if out-of-date
from datetime import datetime
TODAY_DATE = datetime.today()
MAX_DATE_N50 = datetime.strptime(nifty50_db.get_max_date(),'%Y-%m-%d')
MAX_DATE_DJ = datetime.strptime(dow_jones_db.get_max_date(),'%Y-%m-%d')

if (TODAY_DATE > MAX_DATE_N50):
    nifty50_db.update()
if (TODAY_DATE > MAX_DATE_DJ):  
    dow_jones_db.update()

# Recommender
nifty50 = StockRecommender('NIFTY50', nifty50_db)
dow_jones = StockRecommender('DJIA', dow_jones_db)

nifty50.recommender()
dow_jones.recommender()
