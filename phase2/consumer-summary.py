from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String
import statistics


# Load enviornment variables from .env file
dotenv_local_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True) 

# Create Declartive base class maintains catalog of classes and tables
Base = declarative_base()

# Create Transaction class; transaction table mapped to this class
# transaction table stores records for end-users of our application
class Transaction(Base):
    __tablename__ = 'transaction'
    __table_args__ = {"schema": "kafka"}
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.engine = create_engine('postgresql://' + os.environ.get('POSTGRES_USER') + ':' + os.environ.get('POSTGRES_PASSWORD') + '@localhost:5433/postgres')
        Session = sessionmaker(bind=self.engine)
        Session.configure(bind=self.engine)
        self.session = Session()    
        #Go back to the readme.
        self.avgDep = 0
        self.avgWth = 0
        self.stdDevDep = None
        self.stdDevWth = None
        self.deposits = []
        self.withdrawals = []
        self.summary = {}

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL using SQLalchemy
            #message_sql = Transaction(custid=message['custid'], type=message['type'], date=message['date'], amt=message['amt'])
            #self.session.add(message_sql)
            #self.session.commit()

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0  

            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.deposits.append(message['amt'])
                self.avgDep = statistics.mean(self.deposits)
                if len(self.deposits) > 1:
                    self.stdDevDep = statistics.stdev(self.deposits)

            else:
                self.custBalances[message['custid']] -= message['amt']
                self.withdrawals.append(message['amt'])
                self.avgWth = statistics.mean(self.withdrawals)
                if len(self.withdrawals) > 1:
                    self.stdDevWth = statistics.stdev(self.withdrawals)
            
            self.summary = {'avg_deposit':round(self.avgDep,2), 'avg_withdrawal':round(self.avgWth,2)}
            if len(self.deposits) > 1:
                self.summary['stddev_deposits'] = round(self.stdDevDep,2)
            if len(self.withdrawals) > 1:
                self.summary['stddev_withdrawals'] = round(self.stdDevWth,2)

            print(self.summary)
            #print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()